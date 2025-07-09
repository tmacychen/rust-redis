/*


*/

use dashmap::DashMap;
use std::{hash::Hash, time::Instant};

pub struct DataBase<K, V> {
    kv_db: DashMap<K, V>,
    expiry_time: DashMap<K, (String, Instant)>,
}

impl<K: Eq + Hash + Clone, V: Clone> DataBase<K, V> {
    pub fn new() -> Self {
        DataBase {
            kv_db: DashMap::new(),
            expiry_time: DashMap::new(),
        }
    }
    pub fn kv_insert(&self, k: K, v: V) -> Option<V> {
        self.kv_db.insert(k, v)
    }
    pub fn kv_get(&self, k: &K) -> Option<V> {
        self.kv_db.get(k).map(|v| v.clone())
    }
    pub fn set_expiry_time(
        &self,
        k: K,
        (exp_str, t): (String, Instant),
    ) -> Option<(String, Instant)> {
        self.expiry_time.insert(k, (exp_str, t))
    }

    pub fn get_expiry_time(&self, k: &K) -> Option<(String, Instant)> {
        self.expiry_time.get(k).map(|t| t.clone())
    }
}
