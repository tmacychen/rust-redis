/*


*/

use dashmap::DashMap;
use std::time::Instant;

pub type ExpireTime = (String, Instant);

pub type ValueType = (String, Option<ExpireTime>);

pub struct DataBase {
    kv_db: DashMap<String, ValueType>,
    // expiry_time: DashMap<K, (String, Instant)>,
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            kv_db: DashMap::new(),
        }
    }
    pub fn insert(&self, k: String, v: ValueType) -> Option<ValueType> {
        self.kv_db.insert(k, v)
    }
    pub fn delete(&self, k: &str) -> Option<(String, ValueType)> {
        self.kv_db.remove(k)
    }
    pub fn get(&self, k: &str) -> Option<ValueType> {
        self.kv_db.get(k).map(|v| v.clone())
    }
    pub fn keys(&self) -> Vec<String> {
        self.kv_db.iter().map(|entry| entry.key().clone()).collect()
    }
}
