use dashmap::DashMap;
use std::time::Instant;

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
pub const DB_NUM: u64 = 0;

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
    Seconds(u32),
    Milliseconds(u64),
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
    pub databases: DashMap<u64, DashMap<String, KeyValue>>,
}

impl RdbFile {
    // 创建一个新的RDB文件
    pub fn new(version: u32) -> Self {
        Self {
            version: version,
            aux_fields: DashMap::new(),
            databases: DashMap::new(),
        }
    }

    // 设置辅助字段
    pub fn set_aux(&mut self, key: String, value: String) {
        self.aux_fields.insert(key, value);
    }

    // 异步获取指定数据库中的键值对
    pub async fn get(&self, db: u64, key: &str) -> Option<KeyValue> {
        log::debug!("database is {:?} db_num is {}", self.databases, db);
        log::debug!(
            "get debug :{:?}",
            self.databases.get(&db).expect("get db error").get(key)
        );
        match self.databases.get(&db).expect("get db error").get(key) {
            Some(v) => Some(v.value().clone()),
            None => None,
        }
    }

    // 异步设置键值对，如果已存在则更新
    pub async fn insert(
        &mut self,
        db: u64,
        key: String,
        value: RedisValue,
        expiry: Option<(Expiry, Instant)>,
    ) {
        self.databases
            .entry(db)
            .or_insert(DashMap::new())
            .entry(key.clone())
            .or_insert(KeyValue {
                value: value,
                expiry: expiry,
            });
        log::debug!(
            "insert debug :{:?}",
            self.databases
                .get(&DB_NUM)
                .expect("get db error")
                .get(&key)
                .expect("get key error")
                .value()
        )
    }

    // 异步删除指定的键
    pub async fn delete(&mut self, db: u64, key: &str) -> bool {
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
    pub async fn keys(&self, db: u64) -> Option<Vec<String>> {
        if let Some(database) = self.databases.get(&db) {
            Some(database.iter().map(|entry| entry.key().clone()).collect())
        } else {
            None
        }
    }

    // TODO: rewrite for get size
    // 异步获取数据库大小（键值对数量）
    pub async fn dbsize(&self, db: u64) -> usize {
        self.databases.get(&db).map(|kvs| kvs.len()).unwrap_or(0)
    }

    // TODO: rewrite for get count
    // 获取数据库数量
    pub fn db_count(&self) -> usize {
        self.databases.len()
    }
}

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use crc64fast::Digest;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

// RDB文件异步解析器
pub struct RdbParser<R: AsyncReadExt + AsyncSeekExt + Unpin> {
    reader: R,
    crc: Digest,
}

impl<R: AsyncReadExt + AsyncSeekExt + Unpin> RdbParser<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            crc: Digest::new(),
        }
    }

    // 异步解析整个RDB文件
    pub async fn parse(&mut self) -> Result<RdbFile> {
        // 读取并验证魔数
        let mut magic = [0u8; 5];
        self.read_bytes(&mut magic).await?;

        if &magic != MAGIC_STRING {
            anyhow::bail!("Not a valid RDB file");
        }

        // 读取版本号
        let mut version_bytes = [0u8; 4];
        self.read_bytes(&mut version_bytes).await?;

        let version_str = String::from_utf8_lossy(&version_bytes);
        let version = version_str.parse::<u32>()?;

        let rdb_file = RdbFile::new(version);
        let mut current_db = DB_NUM;

        loop {
            let byte = match self.read_u8().await {
                Ok(b) => b,
                Err(e) => {
                    // 尝试将 anyhow::Error 转换为 io::Error
                    if let Some(io_err) = e.downcast_ref::<io::Error>() {
                        if io_err.kind() == io::ErrorKind::UnexpectedEof {
                            break; // 遇到文件结束，退出循环
                        }
                    }
                    return Err(e); // 其他错误，向上传播
                }
            };

            match byte {
                TYPE_AUX => {
                    // 处理辅助字段 (RDB版本7及以上)
                    if version >= 7 {
                        self.parse_auxiliary_field(&rdb_file).await?;
                    } else {
                        anyhow::bail!("Auxiliary field (0xFA) found in RDB version {}, but it was introduced in version 7", version);
                    }
                }
                TYPE_SELECTDB => {
                    // 数据库选择器
                    current_db = self.read_u8().await? as u64;
                    rdb_file
                        .databases
                        .entry(current_db)
                        .or_insert(DashMap::new());

                    // 检查是否有RESIZEDB字段
                    self.read_u8().await?; // 消耗掉0xFB
                    let mut _ht_size = self.read_length().await?;
                    let _expires_size = self.read_length().await?;

                    // 解析该数据库中的所有键值对
                    while _ht_size > 0 {
                        _ht_size -= 1;
                        let mut key: String = "".to_string();
                        let mut key_value: KeyValue = KeyValue {
                            value: RedisValue::String("".to_string()),
                            expiry: None,
                        };
                        // 解析键值对
                        let byte = self.read_u8().await?;
                        match byte {
                            TYPE_EXPIRETIME | TYPE_EXPIRETIME_MS => {
                                // 处理带过期时间的键值对
                                let expiry = match byte {
                                    0xFD => Expiry::Seconds(self.read_u32::<LittleEndian>().await?),
                                    0xFC => {
                                        Expiry::Milliseconds(self.read_u64::<LittleEndian>().await?)
                                    }
                                    _ => unreachable!(),
                                };
                                // TODO
                                let value_type = self.read_u8().await?;
                                key = self.read_string().await?;
                                let value = self.parse_value(value_type).await?;

                                key_value = KeyValue {
                                    value,
                                    expiry: Some((expiry, Instant::now())),
                                };
                            }
                            // 没有过期时间的键值对
                            _ => {
                                key = self.read_string().await?;
                                let value = self.read_string().await?;
                                log::debug!("{key}:{value}");

                                key_value = KeyValue {
                                    value: RedisValue::String(value),
                                    expiry: None,
                                };
                            }
                        }

                        rdb_file
                            .databases
                            .entry(current_db)
                            .or_insert(DashMap::new())
                            .entry(key)
                            .or_insert(key_value);
                    }
                }
                TYPE_EOF => {
                    break;
                }
                _ => {
                    anyhow::bail!(
                        "Unexpected byte {:02X} at start of database or auxiliary field",
                        byte
                    )
                }
            }
        }

        // 验证CRC64校验和
        let file_size = self.reader.seek(SeekFrom::End(0)).await?;
        self.reader.seek(SeekFrom::Start(file_size - 8)).await?;
        let stored_checksum = self.read_u64::<LittleEndian>().await?;
        let computed_checksum = self.crc.sum64();

        if stored_checksum != computed_checksum {
            anyhow::bail!("CRC64 checksum mismatch");
        }

        Ok(rdb_file)
    }

    // 解析辅助字段 (0xFA标记)
    async fn parse_auxiliary_field(&mut self, rdb_file: &RdbFile) -> Result<()> {
        // 读取键和值（均为Redis字符串类型）

        loop {
            let key = self.read_string().await?;

            // 处理已知字段
            match key.as_str() {
                "redis-ver" => {
                    let value = self.read_string().await?;
                    rdb_file.aux_fields.insert(key.clone(), value.clone());
                }
                "redis-bits" => {
                    let value = self.read_length().await?;
                    log::debug!("redis bits is {value}");
                    if value == 32 {
                        rdb_file.aux_fields.insert(key.clone(), "32".to_string());
                    } else if value == 64 {
                        rdb_file.aux_fields.insert(key.clone(), "64".to_string());
                    }
                }
                "ctime" => {
                    let value = self.read_string().await?;
                    rdb_file.aux_fields.insert(key.clone(), value.clone());
                }
                "used-mem" => {
                    let value = self.read_string().await?;
                    rdb_file.aux_fields.insert(key.clone(), value.clone());
                }
                _ => {
                    // 忽略未知字段
                    log::debug!("Ignoring unknown auxiliary field: {}", key);
                }
            }
            if self
                .peek_u8()
                .await
                .is_ok_and(|b| b == TYPE_AUX || b == TYPE_SELECTDB)
            {
                break;
            }
        }
        Ok(())
    }

    // 解析不同类型的值
    async fn parse_value(&mut self, value_type: u8) -> Result<RedisValue> {
        match value_type {
            0x00 => {
                // 简单字符串
                let s = self.read_string().await?;
                Ok(RedisValue::String(s))
            }
            0x01 => {
                // 列表
                let len = self.read_length().await?;
                let mut list = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    list.push(self.read_string().await?);
                }
                Ok(RedisValue::List(list))
            }
            0x02 => {
                // 哈希
                let len = self.read_length().await?;
                let mut hash = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let key = self.read_string().await?;
                    let value = self.read_string().await?;
                    hash.push((key, value));
                }
                Ok(RedisValue::Hash(hash))
            }
            0x03 => {
                // 集合
                let len = self.read_length().await?;
                let mut set = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    set.push(self.read_string().await?);
                }
                Ok(RedisValue::Set(set))
            }
            0x04 => {
                // 有序集合
                let len = self.read_length().await?;
                let mut sorted_set = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let element = self.read_string().await?;
                    let score = self.read_double().await?;
                    sorted_set.push((element, score));
                }
                Ok(RedisValue::SortedSet(sorted_set))
            }
            // 其他类型的解析实现...
            _ => anyhow::bail!("Unsupported value type: {}", value_type),
        }
    }

    // 读取字符串
    async fn read_string(&mut self) -> Result<String> {
        let len = self.read_length().await?;
        log::debug!("read length is {len}");

        // 添加最大长度限制，防止内存溢出
        if len > 1024 * 1024 {
            // 限制最大1MB
            anyhow::bail!("String length exceeds maximum allowed size: {}", len);
        }

        let mut bytes = vec![0u8; len as usize];
        self.read_bytes(&mut bytes).await?;
        String::from_utf8(bytes).context("Failed to convert bytes to String")
    }

    // 读取长度编码
    async fn read_length(&mut self) -> Result<u64> {
        let first_byte = self.read_u8().await?;

        // 提取前两位比特
        let prefix = (first_byte & 0xC0) >> 6;

        match prefix {
            0 => {
                // 00: 接下来的6位表示长度 (0-63)
                Ok((first_byte & 0x3F) as u64)
            }
            1 => {
                // 01: 读取一个额外字节，组合的14位表示长度 (64-16383)
                let second_byte = self.read_u8().await?;
                Ok((((first_byte & 0x3F) as u64) << 8) | (second_byte as u64))
            }
            2 => {
                // 10: 丢弃剩余6位，接下来4字节表示长度
                let len = self.read_u32::<LittleEndian>().await?;
                Ok(len as u64)
            }
            3 => {
                // 11: 特殊格式编码
                let special_code = first_byte & 0x3F;

                match special_code {
                    0 => {
                        // 8位有符号整数
                        let value = self.read_i8().await?;
                        Ok(value as u64)
                    }
                    1 => {
                        // 16位有符号整数
                        let value = self.read_i16::<LittleEndian>().await?;
                        Ok(value as u64)
                    }
                    2 => {
                        // 32位有符号整数
                        let value = self.read_i32::<LittleEndian>().await?;
                        Ok(value as u64)
                    }
                    3 => {
                        // LZF压缩字符串（此处返回压缩标记，实际解析在别处处理）
                        Err(anyhow::anyhow!("LZF compressed string length encoding should be handled in string parsing context"))
                    }
                    _ => {
                        anyhow::bail!("Unsupported special length encoding: {}", special_code)
                    }
                }
            }
            _ => unreachable!(), // 前两位只能是00,01,10,11
        }
    }

    // 读取双精度浮点数
    async fn read_double(&mut self) -> Result<f64> {
        let len = self.read_length().await?;

        if len == 253 {
            // 特殊值: +inf
            Ok(f64::INFINITY)
        } else if len == 254 {
            // 特殊值: -inf
            Ok(f64::NEG_INFINITY)
        } else if len == 255 {
            // 特殊值: nan
            Ok(f64::NAN)
        } else if len == 8 {
            // 正常的64位双精度浮点数
            Ok(self.read_f64::<LittleEndian>().await?)
        } else {
            anyhow::bail!("Unsupported double length: {}", len)
        }
    }

    // 辅助读取方法，同时更新CRC
    async fn read_bytes(&mut self, buf: &mut [u8]) -> Result<usize> {
        let bytes_read = self.reader.read(buf).await?;
        self.crc.write(&buf[0..bytes_read]);
        Ok(bytes_read)
    }

    // 读取单个字节，同时更新CRC
    async fn read_u8(&mut self) -> Result<u8> {
        let byte = self.reader.read_u8().await?;
        self.crc.write(&[byte]);
        Ok(byte)
    }

    // 读取整数，同时更新CRC
    async fn read_u32<T: byteorder::ByteOrder>(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(T::read_u32(&buf))
    }

    // 读取长整数，同时更新CRC
    async fn read_u64<T: byteorder::ByteOrder>(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(T::read_u64(&buf))
    }

    // // 读取长整数，同时更新CRC
    // async fn read_u128<T: byteorder::ByteOrder>(&mut self) -> Result<u128> {
    //     let mut buf = [0u8; 16];
    //     self.read_bytes(&mut buf).await?;
    //     Ok(T::read_u128(&buf))
    // }

    // 读取有符号整数，同时更新CRC
    async fn read_i8(&mut self) -> Result<i8> {
        let byte = self.read_u8().await?;
        Ok(byte as i8)
    }

    // 读取有符号短整数，同时更新CRC
    async fn read_i16<T: byteorder::ByteOrder>(&mut self) -> Result<i16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf).await?;
        Ok(T::read_i16(&buf))
    }

    // 读取有符号整数，同时更新CRC
    async fn read_i32<T: byteorder::ByteOrder>(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(T::read_i32(&buf))
    }

    // 读取双精度浮点数，同时更新CRC
    async fn read_f64<T: byteorder::ByteOrder>(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(T::read_f64(&buf))
    }

    // 查看下一个字节但不消耗它
    async fn peek_u8(&mut self) -> Result<u8> {
        let current_pos = self.reader.seek(SeekFrom::Current(0)).await?;
        let byte = self.reader.read_u8().await?;
        self.reader.seek(SeekFrom::Start(current_pos)).await?;
        Ok(byte)
    }
}

// RDB文件异步写入器
pub struct RdbWriter<W: AsyncWriteExt + AsyncSeekExt + Unpin> {
    writer: W,
    crc: Digest,
}

impl<W: AsyncWriteExt + AsyncSeekExt + Unpin> RdbWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            crc: Digest::new(),
        }
    }

    // 异步写入整个RDB文件
    pub async fn write(&mut self, rdb_file: &RdbFile) -> Result<()> {
        // 写入魔数和版本号
        self.write_bytes(b"REDIS").await?;
        self.write_bytes(format!("{:04}", rdb_file.version).as_bytes())
            .await?;

        // 写入辅助字段
        for e in rdb_file.aux_fields.iter() {
            self.write_u8(0xFA).await?;
            self.write_string(e.key()).await?;
            self.write_string(e.value()).await?;
        }

        // 写入各个数据库
        for e in rdb_file.databases.iter() {
            // 写入数据库选择器
            let db_num = e.key();
            let db_map = e.value();

            self.write_u8(0xFE).await?;
            self.write_length(*db_num as u64).await?;

            // 写入RESIZEDB字段 (简化处理，使用估算值)
            self.write_u8(0xFB).await?;
            self.write_length(db_map.len() as u64).await?; // 哈希表大小
            self.write_length(0).await?; // 过期哈希表大小

            // 写入键值对
            for e in db_map.iter() {
                let key = e.key();
                let kv = e.value();
                if let Some((expiry, _timestamp)) = &kv.expiry {
                    match expiry {
                        Expiry::Seconds(secs) => {
                            self.write_u8(0xFD).await?;
                            self.write_u32::<LittleEndian>(*secs).await?;
                        }
                        Expiry::Milliseconds(millis) => {
                            self.write_u8(0xFC).await?;
                            self.write_u64::<LittleEndian>(*millis).await?;
                        }
                    }
                }

                // 写入值类型和键
                self.write_value_type(&kv.value).await?;
                self.write_string(key).await?;

                // 写入值
                self.write_value(&kv.value).await?;
            }
        }

        // 写入文件结束标记
        self.write_u8(0xFF).await?;

        // 计算并写入CRC64校验和
        let checksum = self.crc.sum64();
        self.writer.seek(SeekFrom::End(0)).await?;
        self.write_u64::<LittleEndian>(checksum).await?;

        Ok(())
    }

    // 写入值类型
    async fn write_value_type(&mut self, value: &RedisValue) -> Result<()> {
        let type_byte = match value {
            RedisValue::String(_) => 0x00,
            RedisValue::List(_) => 0x01,
            RedisValue::Hash(_) => 0x02,
            RedisValue::Set(_) => 0x03,
            RedisValue::SortedSet(_) => 0x04,
        };
        self.write_u8(type_byte).await
    }

    // 写入值
    async fn write_value(&mut self, value: &RedisValue) -> Result<()> {
        match value {
            RedisValue::String(s) => self.write_string(s).await,
            RedisValue::List(items) => {
                self.write_length(items.len() as u64).await?;
                for item in items {
                    self.write_string(item).await?;
                }
                Ok(())
            }
            RedisValue::Hash(fields) => {
                self.write_length(fields.len() as u64).await?;
                for (k, v) in fields {
                    self.write_string(k).await?;
                    self.write_string(v).await?;
                }
                Ok(())
            }
            RedisValue::Set(items) => {
                self.write_length(items.len() as u64).await?;
                for item in items {
                    self.write_string(item).await?;
                }
                Ok(())
            }
            RedisValue::SortedSet(items) => {
                self.write_length(items.len() as u64).await?;
                for (element, score) in items {
                    self.write_string(element).await?;
                    self.write_double(*score).await?;
                }
                Ok(())
            }
        }
    }

    // 写入字符串
    async fn write_string(&mut self, s: &str) -> Result<()> {
        let bytes = s.as_bytes();
        self.write_length(bytes.len() as u64).await?;
        self.write_bytes(bytes).await
    }

    // 写入长度编码
    async fn write_length(&mut self, len: u64) -> Result<()> {
        if len < 64 {
            // 短长度 (0-63)
            self.write_u8(len as u8).await
        } else if len < 16384 {
            // 中等长度 (64-16383)
            self.write_u8(0x40 | ((len >> 8) as u8)).await?;
            self.write_u8((len & 0xFF) as u8).await
        } else {
            // 长长度
            self.write_u8(0x80).await?;
            self.write_u64::<LittleEndian>(len).await
        }
    }

    // 写入双精度浮点数
    async fn write_double(&mut self, value: f64) -> Result<()> {
        if value.is_infinite() && value.is_sign_positive() {
            // +inf
            self.write_length(253).await
        } else if value.is_infinite() && value.is_sign_negative() {
            // -inf
            self.write_length(254).await
        } else if value.is_nan() {
            // nan
            self.write_length(255).await
        } else {
            // 正常的64位双精度浮点数
            self.write_length(8).await?;
            self.write_f64::<LittleEndian>(value).await
        }
    }

    // 辅助写入方法，同时更新CRC
    async fn write_bytes(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(buf).await?;
        let _ = self.crc.write(buf);
        Ok(())
    }

    // 写入单个字节，同时更新CRC
    async fn write_u8(&mut self, byte: u8) -> Result<()> {
        self.writer.write_all(&[byte]).await?;
        let _ = self.crc.write(&[byte]);
        Ok(())
    }

    // 写入整数，同时更新CRC
    async fn write_u32<T: byteorder::ByteOrder>(&mut self, value: u32) -> Result<()> {
        let mut buf = [0u8; 4];
        T::write_u32(&mut buf, value);
        self.write_bytes(&buf).await
    }

    // 写入长整数，同时更新CRC
    async fn write_u64<T: byteorder::ByteOrder>(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; 8];
        T::write_u64(&mut buf, value);
        self.write_bytes(&buf).await
    }

    // 写入双精度浮点数，同时更新CRC
    async fn write_f64<T: byteorder::ByteOrder>(&mut self, value: f64) -> Result<()> {
        let mut buf = [0u8; 8];
        T::write_f64(&mut buf, value);
        self.write_bytes(&buf).await
    }
}
