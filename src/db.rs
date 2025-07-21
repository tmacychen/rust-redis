use dashmap::DashMap;
use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc64fast::Digest;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

// pub type ExpireTime = (String, Instant);
// pub type ValueType = (String, Option<ExpireTime>);

#[derive(Debug, Clone, Default)]
pub struct Dbconf {
    dir: String,
    db_filename: String,
}

impl Dbconf {
    pub fn new() -> Self {
        Dbconf {
            ..Default::default()
        }
    }
    pub fn set(&mut self, dir: String, db_filename: String) {
        self.dir = dir;
        self.db_filename = db_filename;
    }
    pub fn get_dir(&self) -> String {
        self.dir.clone()
    }
    pub fn get_db_filename(&self) -> String {
        self.db_filename.clone()
    }
}

// RDB文件中的常量定义
const MAGIC_STRING: &[u8] = b"REDIS";
pub const RDB_VERSION: u32 = 9;

// RDB文件中的特殊字节
const TYPE_AUX: u8 = 0xFA;
const TYPE_RESIZEDB: u8 = 0xFB;
const TYPE_SELECTDB: u8 = 0xFE;
const TYPE_EOF: u8 = 0xFF;
const TYPE_EXPIRETIME: u8 = 0xFD;
const TYPE_EXPIRETIME_MS: u8 = 0xFC;

// DB number for test
pub const DB_NUM: u32 = 0;

// RDB文件中的数据类型
#[derive(Debug, Clone, PartialEq)]
pub enum RDB_V_Type {
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Zipmap,
    Ziplist,
    SetInts,
    SortedSetZipmap,
    HashZiplist,
    ListQuicklist,
    Module,
    Module2,
    StreamListPacks,
}
// 过期时间类型
#[derive(Debug, Clone, PartialEq)]
pub enum Expiry {
    Seconds(u64),
    Milliseconds(u128),
}

// 支持过期时间的键值对
#[derive(Debug, Clone)]
pub struct KeyValue {
    // pub key: String,
    pub value: RedisValue,
    pub expiry: Option<(Expiry, Instant)>,
}

// Redis支持的数据结构
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Set(Vec<String>),
    SortedSet(Vec<(String, f64)>),
    Hash(Vec<(String, String)>),
    // Zipmap(Vec<(String, String)>),
    // Ziplist(Vec<Vec<u8>>),
    // SetInts(Vec<i64>),
    // SortedSetZipmap(Vec<(String, f64)>),
    // HashZiplist(Vec<(String, String)>),
    // ListQuicklist(Vec<String>),
    // Module(u32, Vec<u8>),
    // StreamListPacks(Vec<u8>),
}

// RDB file structure
/*
// ----------------------------#
52 45 44 49 53              # Magic String "REDIS"
30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
----------------------------
FA                          # Auxiliary field
$string-encoded-key         # May contain arbitrary metadata
$string-encoded-value       # such as Redis version, creation time, used memory, ...
----------------------------
FE 00                       # Indicates database selector. db number = 00
FB                          # Indicates a resizedb field
$length-encoded-int         # Size of the corresponding hash table
$length-encoded-int         # Size of the corresponding expire hash table
----------------------------# Key-Value pair starts
FD $unsigned-int            # "expiry time in seconds", followed by 4 byte unsigned int
$value-type                 # 1 byte flag indicating the type of value
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value, encoding depends on $value-type
----------------------------
FC $unsigned long           # "expiry time in ms", followed by 8 byte unsigned long
$value-type                 # 1 byte flag indicating the type of value
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value, encoding depends on $value-type
----------------------------
$value-type                 # key-value pair without expiry
$string-encoded-key
$encoded-value
----------------------------
FE $length-encoding         # Previous db ends, next db starts.
----------------------------
...                         # Additional key-value pairs, databases, ...

FF                          ## End of RDB file indicator
8-byte-checksum             ## CRC64 checksum of the entire file.
*/

#[derive(Debug, Clone)]
pub struct RdbFile {
    pub version: u32,
    pub aux_fields: DashMap<String, String>,
    pub databases: DashMap<u32, DashMap<String, KeyValue>>,
}

impl RdbFile {
    // 创建一个新的RDB文件
    pub fn new(version: u32) -> Self {
        Self {
            version,
            aux_fields: DashMap::new(),
            databases: DashMap::new(),
        }
    }

    // 设置辅助字段
    pub fn set_aux(&mut self, key: String, value: String) {
        self.aux_fields.insert(key, value);
    }

    // 异步获取指定数据库中的键值对
    pub async fn get(&self, db: u32, key: &str) -> Option<KeyValue> {
        match self.databases.get(&db).expect("get db error").get(key) {
            Some(v) => Some(v.value().clone()),
            None => None,
        }
    }

    // 异步设置键值对，如果已存在则更新
    pub async fn insert(
        &mut self,
        db: u32,
        key: String,
        value: RedisValue,
        expiry: Option<(Expiry, Instant)>,
    ) {
        self.databases.entry(db).or_insert(DashMap::new()).insert(
            key,
            KeyValue {
                value: value,
                expiry: expiry,
            },
        );
    }

    // 异步删除指定的键
    pub async fn delete(&mut self, db: u32, key: &str) -> bool {
        if let Some(db_entry) = self.databases.get_mut(&db) {
            if let Some((k, _)) = db_entry.remove(key) {
                if k == key {
                    return true;
                }
            }
        }
        false
    }

    // 异步获取所有键
    pub async fn keys(&self, db: u32) -> Vec<String> {
        self.databases
            .get(&db)
            .expect("get db error")
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    // TODO: rewrite for get size
    // 异步获取数据库大小（键值对数量）
    pub async fn dbsize(&self, db: u32) -> usize {
        self.databases.get(&db).map(|kvs| kvs.len()).unwrap_or(0)
    }

    // TODO: rewrite for get count
    // 获取数据库数量
    pub fn db_count(&self) -> usize {
        self.databases.len()
    }
}

// #[derive(Default)]
// pub struct DataBase {
//     kv_db: DashMap<String, ValueType>,
//     // db_file: (String, String, Option<File>), // (dir,db_file_name,db_file)
// }

// impl DataBase {
//     pub fn new() -> Self {
//         DataBase {
//             kv_db: DashMap::new(),
//             ..Default::default()
//         }
//     }
//     pub fn init_db_file(&mut self, dir: &str, db_file_name: &str) {
//         let mut file_name = PathBuf::new();
//         file_name.push(dir);
//         file_name.push(db_file_name);
//         log::debug!("file name is {}", file_name.as_path().to_str().unwrap());

//         // self.db_file = (
//         //     dir.to_string(),
//         //     db_file_name.to_string(),
//         //     Some(File::create(file_name).expect("can't create a db file")),
//         // );
//     }
//     pub fn insert(&self, k: String, v: ValueType) -> Option<ValueType> {
//         self.kv_db.insert(k, v)
//     }
//     pub fn delete(&self, k: &str) -> Option<(String, ValueType)> {
//         self.kv_db.remove(k)
//     }
//     pub fn get(&self, k: &str) -> Option<ValueType> {
//         self.kv_db.get(k).map(|v| v.clone())
//     }
//     pub fn keys(&self) -> Vec<String> {
//         self.kv_db.iter().map(|entry| entry.key().clone()).collect()
//     }
// }
