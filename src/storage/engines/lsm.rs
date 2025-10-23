use super::KvEngine;
use std::collections::HashMap;

pub struct LsmStore {
    map: HashMap<Vec<u8>, Vec<u8>>
}

impl LsmStore {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.map.insert(key.to_vec(), value.to_vec());
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.map.get(key)
    }

    pub fn delete(&mut self, key: &[u8]) {
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