use std::{fs::{File, OpenOptions}, io::{BufReader, BufWriter, Read, Write, Result}, path::{Path, PathBuf}};


/*
wal is a sequential append only log
write immediately on every write (put/delete)
cleanup/compact after memtable flushes
wal is temporary
sequential writes, sequential reads during recovery
*/

// Considerations for WAL Design: https://x.com/jorandirkgreef/status/1892109953608958252struct Wal {
    // length: u8,
    // index: u8,
    // data: u32,
    // // checksum: u?


/// Tag indicating a Put entry in the Write-Ahead Log.
const PUT_TAG: u8 = 0;

/// Tag indicating a Delete entry in the Write-Ahead Log.
const DELETE_TAG: u8 = 1;

/// Size of the operation tag byte (1 byte for u8)
const TAG_SIZE: usize = 1;

/// Size of length prefix fields (4 bytes for u32)
const LENGTH_FIELD_SIZE: usize = 4;

/// Maximum size of a single WAL entry (1MB)
const MAX_ENTRY_SIZE: usize = 1 * 1024 * 1024;


/// Represents an entry in the Write-Ahead Log.
#[derive(Debug, Clone)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Write-Ahead Log for durable storage of operations.
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>
}

impl Wal {
    /// Opens or creates a WAL file at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self { path, writer })
    }

    /// Calculate the encoded size of a WAL entry (without length prefix)
    fn encoded_size(entry: &WalEntry) -> usize {
        match entry {
            WalEntry::Put { key, value } => {
                TAG_SIZE +
                LENGTH_FIELD_SIZE + // key length field
                key.len() +
                LENGTH_FIELD_SIZE + // value length field
                value.len()
            }
            WalEntry::Delete { key } => {
                TAG_SIZE +
                LENGTH_FIELD_SIZE + // key length field
                key.len()
            }
        }
    }

    /// Write an encoded entry to the writer (without length prefix)
    fn write_entry(&mut self, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::Put { key, value } => {
                self.writer.write_all(&[PUT_TAG])?;
                self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
                self.writer.write_all(key)?;
                self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
                self.writer.write_all(value)?;
            }
            WalEntry::Delete { key } => {
                self.writer.write_all(&[DELETE_TAG])?;
                self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
                self.writer.write_all(key)?;
            }
        }
        Ok(())
    }

    /// Appends an entry to the WAL and flushes it to disk for durability.
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        // Calculate entry size and validate
        let entry_size = Self::encoded_size(entry);

        if entry_size > MAX_ENTRY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Entry size {} exceeds maximum {}", entry_size, MAX_ENTRY_SIZE),
            ));
        }

        // Write length prefix first (format: [length: u32][entry_data...])
        self.writer.write_all(&(entry_size as u32).to_le_bytes())?;
        
        // Write entry data
        self.write_entry(entry)?;
        
        // Flush for durability
        self.writer.flush()?;
        
        Ok(())
    }

    /// Convenience method to append a Put entry
    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.append(&WalEntry::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    /// Convenience method to append a Delete entry
    pub fn append_delete(&mut self, key: &[u8]) -> Result<()> {
        self.append(&WalEntry::Delete {
            key: key.to_vec(),
        })
    }

    /// Parse a single entry from a buffer
    /// Format: [tag: 1 byte][key_len: 4 bytes][key][value_len: 4 bytes][value] for Put
    ///         [tag: 1 byte][key_len: 4 bytes][key] for Delete
    fn parse_entry(buffer: &[u8]) -> Result<WalEntry> {
        if buffer.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Entry buffer is empty",
            ));
        }
        let tag = buffer[0];
        let mut offset = TAG_SIZE;
        
        // Read key length
        if offset + LENGTH_FIELD_SIZE > buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Entry buffer too short for key length",
            ));
        }
        
        let key_len = u32::from_le_bytes(
            buffer[offset..offset + LENGTH_FIELD_SIZE].try_into().unwrap()
        ) as usize;
        offset += LENGTH_FIELD_SIZE;
        
        // Read key
        if offset + key_len > buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Entry buffer too short for key",
            ));
        }
        let key = buffer[offset..offset + key_len].to_vec();
        offset += key_len;
        
        match tag {
            PUT_TAG => {
                // Read value length
                if offset + LENGTH_FIELD_SIZE > buffer.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Entry buffer too short for value length",
                    ));
                }
                let value_len = u32::from_le_bytes(
                    buffer[offset..offset + LENGTH_FIELD_SIZE].try_into().unwrap()
                ) as usize;
                offset += LENGTH_FIELD_SIZE;
                
                // Read value
                if offset + value_len > buffer.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Entry buffer too short for value",
                    ));
                }
                let value = buffer[offset..offset + value_len].to_vec();
                
                Ok(WalEntry::Put { key, value })
            }
            DELETE_TAG => {
                Ok(WalEntry::Delete { key })
            }
            _ => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Unknown tag: {}", tag),
                ))
            }
        }
    }

    /// Replay all entries from a WAL file
    /// Opens the file for reading and parses all entries sequentially
    pub fn replay<P: AsRef<Path>>(path: P) -> Result<Vec<WalEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        loop {
            // Read the length prefix
            let mut length_bytes = [0u8; LENGTH_FIELD_SIZE];
            match reader.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
            
            let entry_length = u32::from_le_bytes(length_bytes) as usize;
            
            if entry_length > MAX_ENTRY_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Entry length {} exceeds maximum {}", entry_length, MAX_ENTRY_SIZE),
                ));
            }
            
            // Read and parse the entry
            let mut entry_buffer = vec![0u8; entry_length];
            reader.read_exact(&mut entry_buffer)?;
            let entry = Self::parse_entry(&entry_buffer)?;
            entries.push(entry);
        }
        
        Ok(entries)
    }

}


impl Drop for Wal {
    fn drop(&mut self) {
        // Try to flush on drop, but log error if it fails rather than panicking
        // In production, you might want to use a logging framework here
        if let Err(e) = self.writer.flush() {
            eprintln!("Warning: Failed to flush WAL on drop: {}", e);
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal() -> Result<()> {
        let path = "example.wal";
        
        // Clean up any existing file from previous test runs
        std::fs::remove_file(path).ok();

        // Append a few records using convenience methods
        {
            let mut wal = Wal::open(path)?;
            println!("created file {}", path);
            wal.append_put(b"key1", b"value1")?;
            wal.append_put(b"key2", b"value2")?;
            wal.append_delete(b"key1")?;
        }

        // Replay them
        {
            let entries = Wal::replay(path)?;
            assert_eq!(entries.len(), 3);
            
            // Verify entries
            match &entries[0] {
                WalEntry::Put { key, value } => {
                    assert_eq!(key, b"key1");
                    assert_eq!(value, b"value1");
                }
                _ => panic!("Expected Put entry"),
            }
            
            match &entries[1] {
                WalEntry::Put { key, value } => {
                    assert_eq!(key, b"key2");
                    assert_eq!(value, b"value2");
                }
                _ => panic!("Expected Put entry"),
            }
            
            match &entries[2] {
                WalEntry::Delete { key } => {
                    assert_eq!(key, b"key1");
                }
                _ => panic!("Expected Delete entry"),
            }
        }

        // Cleanup
        std::fs::remove_file(path).ok();
        Ok(())
    }
}
