pub mod btree;
pub mod lsm;

/// Common trait for key-value storage engine
pub trait KvEngine {
    /// Create a new instance of the storage engine
    fn new() -> Self;

    /// Insert of update a key-value pair
    fn put(&mut self, key: &[u8], value: &[u8]);

    /// Get a value by key
    fn get(&self, key: &[u8]) -> Option<&Vec<u8>>;

    /// Delete a key-value pair
    fn delete(&mut self, key: &[u8]);
}
