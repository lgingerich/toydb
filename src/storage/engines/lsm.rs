/*
========================================================================
put(k, v) -> [ WAL ] -> [ MemTable ] --flush--> [ SSTable ]

[ MemTable ]: in-memory sorted map, flush when full
[ SSTable ]: immutable sorted file on disk
[ WAL ]: append record to on disk WAL for durability

========================================================================
get(k) -> [ MemTable ] -> [ SSTable ] -> result

[ MemTable ]: check for latest value or tombstone
[ SSTable]: search oldest-to-newest files on disk, return value

========================================================================
delete(k) -> [ WAL ] -> [ MemTable (tombstone) ] --flush--> [ SSTable ]

[ MemTable ]: insert tombstone marker
[ SSTable ]: store tombstone entry (overwrites older data)
[ WAL ]: append tombstone

========================================================================
*/

use super::KvEngine;
use std::collections::HashMap;

struct MemTable {
    memtable: HashMap<Vec<u8>, Vec<u8>>
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            memtable: HashMap::new()
        }
    }

    pub fn insert() {}

    pub fn get() {}

    pub fn flush() {}
}

struct Wal {
    wal: PathBuf
}

impl Wal {
    pub fn new() -> Self {
        Self {
            wal: PathBuf::new()
        }
    }

    pub fn insert_put(&mut self, key: &[u8], value: &[u8]) {
        self.insert(WalEntry::Put { key, value })
    }

    pub fn insert_delete(&mut self, key: &[u8]) {
        self.insert(WalEntry::Delete { key })
    }

    fn insert(&mut self, entry: WalEntry) {
            
    }

    pub fn replay() {
    //    loop over self.get()
    }

    fn get() {}
}

struct SSTable {
    path: PathBuf,
    level: u8,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    size: u64,
}

impl SSTable {
    pub fn new(path: PathBuf, level: u8) -> Self {
        Self {
            path,
            level,
            min_key: Vec::new(),
            max_key: Vec::new(),
            size: 0,
        }
    }

    pub fn write() {}

    pub fn get() {}
}

pub struct LsmStore {
    memtable: MemTable,
    wal: Wal,
    sstable: Vec<SSTable>,
}

impl LsmStore {
    pub fn new() -> Self {
        Self { 
            memtable: MemTable::new(),
            wal: Wal::new(),
            sstable: Vec::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        // Append key-value to WAL
        self.wal.append_put(key, value)?;

        // Update MemTable
        self.memtable.insert(key, value);

        // Flush MemTable if needed
        if self.memtable.size() > self.max_memtable_size {
            self.flush_memtable()?;
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        // Check for key in MemTable first
        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        // Check for key in SSTables next
        if let Some(value) = self.sstable.get(key) {
            return Some(value);
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        // Write to WAL
        self.wal.append_delete(key, value)?;

        // Update MemTable
        self.memtable.insert(key, value);


        self.map.remove(key);
    }
}

impl KvEngine for LsmStore {
    fn new() -> Self {
        Self::new()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.put(key, value);
    }

    fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.get(key)
    }

    fn delete(&mut self, key: &[u8]) {
        self.delete(key);
    }
}



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
