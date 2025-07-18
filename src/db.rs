/*


*/

use std::{fs::File, path::PathBuf, time::Instant};

use dashmap::DashMap;

pub type ExpireTime = (String, Instant);

pub type ValueType = (String, Option<ExpireTime>);

#[derive(Default)]
pub struct DataBase {
    kv_db: DashMap<String, ValueType>,
    db_file: (String, String, Option<File>), // (dir,db_file_name,db_file)
}

impl DataBase {
    pub fn new() -> Self {
        DataBase {
            kv_db: DashMap::new(),
            ..Default::default()
        }
    }
    pub fn init_db_file(&mut self, dir: &str, db_file_name: &str) {
        let mut file_name = PathBuf::new();
        file_name.push(dir);
        file_name.push(db_file_name);
        log::debug!("file name is {}", file_name.as_path().to_str().unwrap());

        self.db_file = (
            dir.to_string(),
            db_file_name.to_string(),
            Some(File::create(file_name).expect("can't create a db file")),
        );
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
