use std::time;

use crate::{
    db::{Dbconf, Expiry, KeyValue, RdbFile, RedisValue, DB_NUM},
    server::Server,
};
use anyhow::{bail, Result};
use resp_protocol::{ArrayBuilder, BulkString, Error, RespType, SimpleString, NULL_BULK_STRING};

#[derive(Clone, Debug)]
pub struct Ping;

#[derive(Clone, Debug)]
pub struct Echo(BulkString);

pub struct Get<'a>(String, &'a mut RdbFile);

pub struct Set<'a>(String, KeyValue, &'a mut RdbFile);

pub struct Keys<'a>(&'a [&'a [u8]], &'a RdbFile);

impl Ping {
    fn exec(&self) -> Result<Vec<u8>> {
        Ok(SimpleString::new(b"PONG").bytes().to_vec())
    }
}
impl Echo {
    fn exec(&self) -> Result<Vec<u8>> {
        Ok(self.0.bytes().to_vec())
    }
}
impl Get<'_> {
    async fn exec<'a>(&'a mut self) -> Result<Vec<u8>> {
        let db: &mut RdbFile = self.1;

        if let Some(value) = db.get(DB_NUM, &self.0).await {
            match value.expiry {
                Some((exp_t, instant_time)) => match exp_t {
                    Expiry::Milliseconds(t) => {
                        log::debug!(
                            "exp_t:{},t.elapsed:{}",
                            t,
                            instant_time.elapsed().as_millis()
                        );
                        if t < instant_time.elapsed().as_millis() as u64 {
                            log::debug!("delete a key {}", db.delete(0, &self.0).await);

                            Ok(NULL_BULK_STRING.bytes().to_vec())
                        } else {
                            get_value_from_redis_type(&value.value)
                        }
                    }
                    Expiry::Seconds(t) => {
                        log::debug!(
                            "exp_t:{},t.elapsed:{}",
                            t,
                            instant_time.elapsed().as_millis()
                        );
                        if t < instant_time.elapsed().as_secs() as u32 {
                            log::debug!("delete a key {}", db.delete(0, &self.0).await);
                            Ok(NULL_BULK_STRING.bytes().to_vec())
                        } else {
                            get_value_from_redis_type(&value.value)
                        }
                    }
                },
                None => get_value_from_redis_type(&value.value),
            }
        } else {
            Ok(NULL_BULK_STRING.bytes().to_vec())
        }
    }
}
//TODO:other redis type map
fn get_value_from_redis_type(v: &RedisValue) -> Result<Vec<u8>> {
    match v {
        RedisValue::String(s) => {
            log::debug!("get v is {s}");
            Ok(BulkString::new(s.as_bytes()).bytes().to_vec())
        }
        _ => Ok(NULL_BULK_STRING.bytes().to_vec()),
    }
}

impl Set<'_> {
    async fn exec(&mut self) -> Result<Vec<u8>> {
        self.2
            .insert(
                DB_NUM,
                self.0.clone(),
                self.1.value.clone(),
                self.1.expiry.clone(),
            )
            .await;
        Ok(SimpleString::new(b"OK").bytes().to_vec())
    }
}
impl<'a> Keys<'a> {
    pub fn new(cmd: &'a [&[u8]], rdb_file: &'a RdbFile) -> Self {
        Keys(cmd, rdb_file)
    }
    async fn exec(&mut self) -> Result<Vec<u8>> {
        log::debug!("get arg is {:?}", &self.0);
        let mut ret_array = ArrayBuilder::new();

        //"[$2 a*]" or "[$1 *]"
        let (last, first) = self.0.split_last().expect("split keys args error");
        if first[0][1] > b'1' {
            let (star, a) = last.split_last().expect("split * from slice");
            log::debug!(
                "star is {} other is {:?}",
                String::from_utf8_lossy(&[*star]),
                a
            );
            Ok(NULL_BULK_STRING.bytes().to_vec())
        } else {
            if let Some(keys) = self.1.keys(DB_NUM).await {
                for k in keys {
                    ret_array.insert(RespType::BulkString(BulkString::new(k.as_bytes())));
                }
                Ok(ret_array.build().bytes().to_vec())
            } else {
                // Ok(ArrayBuilder::new()
                //     .insert(RespType::SimpleString(SimpleString::new(b"-1")))
                //     .build()
                //     .bytes()
                //     .to_vec())
                // Ok(ArrayBuilder::new)
                Ok(BulkString::new(b"-1").bytes().to_vec())
                // Ok(NULL_BULK_STRING.bytes().to_vec())
            }
        }
    }
}

#[derive(Default)]
pub struct Config<'a> {
    cmd: &'a [&'a [u8]],
    db_conf: Dbconf,
}

impl<'a> Config<'a> {
    pub fn new(cmd: &'a [&[u8]], db_conf: &Dbconf) -> Self {
        Config {
            cmd: cmd,
            db_conf: db_conf.clone(),
        }
    }

    fn exec(&self) -> Result<Vec<u8>> {
        log::debug!("config cmd is {:?}", &self.cmd);
        match self.cmd[1].to_ascii_lowercase().as_slice() {
            b"set" => {
                todo!()
            }
            b"get" => match self.cmd[3].to_ascii_lowercase().as_slice() {
                b"dir" => {
                    let mut ret = ArrayBuilder::new();
                    ret.insert(RespType::BulkString(BulkString::new(b"dir")));
                    ret.insert(RespType::BulkString(BulkString::new(
                        self.db_conf.get_dir().as_bytes(),
                    )));
                    Ok(ret.build().bytes().to_vec())
                }
                b"dbfilename" => {
                    let mut ret = ArrayBuilder::new();
                    ret.insert(RespType::BulkString(BulkString::new(b"dbfilename")));
                    ret.insert(RespType::BulkString(BulkString::new(
                        self.db_conf.get_db_filename().as_bytes(),
                    )));
                    Ok(ret.build().bytes().to_vec())
                }
                _ => Ok(Error::new(b"1").bytes().to_vec()),
            },
            _ => bail!("unknown config sub cmd "),
        }
    }
}

pub async fn from_cmd_to_exec(s: Vec<&[u8]>, arg_len: u8, server: &mut Server) -> Result<Vec<u8>> {
    log::debug!("get s:{:?}", s);
    match s[1].to_ascii_lowercase().as_slice() {
        b"ping" => crate::commands::Ping.exec(),
        b"echo" => {
            // 计算echo后的字符串长度,for create vec
            let len = s[2..].iter().enumerate().fold(
                0,
                |s, (i, &v)| {
                    if i % 2 != 0 {
                        s + v.len()
                    } else {
                        s
                    }
                },
            );
            let mut ret = Vec::with_capacity(len);
            log::debug!("s len is {}", len);
            s[2..].iter().enumerate().for_each(|(i, v)| {
                if i % 2 != 0 {
                    ret.extend_from_slice(v);
                }
            });
            crate::commands::Echo(BulkString::new(ret.as_slice())).exec()
        }
        b"get" => {
            let (_, l) = s[2]
                .split_first()
                .expect("get arg's length  $n split error");
            if s[3].len()
                == String::from_utf8(l.to_vec())
                    .expect("get arg size to utf8 error")
                    .parse()
                    .expect("get arg size parse error")
            {
                log::debug!("get arg size is {} ", s[3].len());
            }
            crate::commands::Get(
                String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                &mut server.storage,
            )
            .exec()
            .await
        }
        b"set" => match arg_len {
            3 => {
                crate::commands::Set(
                    String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                    KeyValue {
                        value: RedisValue::String(
                            String::from_utf8(s[5].to_vec()).expect("convert get arg to string"),
                        ),
                        expiry: None,
                    },
                    &mut server.storage,
                )
                .exec()
                .await
            }
            5 => {
                let time_num = String::from_utf8(s[9].to_vec())
                    .expect("conver time to str ")
                    .parse()
                    .expect("parse time num error");
                log::debug!(" set time is {}", time_num);

                match s[7] {
                    b"px" => {
                        crate::commands::Set(
                            String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                            KeyValue {
                                value: RedisValue::String(
                                    String::from_utf8(s[5].to_vec())
                                        .expect("convert get arg to string"),
                                ),
                                expiry: Some((
                                    Expiry::Milliseconds(time_num),
                                    time::Instant::now(),
                                )),
                            },
                            &mut server.storage,
                        )
                        .exec()
                        .await
                    }
                    b"ex" => {
                        crate::commands::Set(
                            String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                            KeyValue {
                                value: RedisValue::String(
                                    String::from_utf8(s[5].to_vec())
                                        .expect("convert get arg to string"),
                                ),
                                expiry: Some((
                                    Expiry::Seconds(time_num as u64),
                                    time::Instant::now(),
                                )),
                            },
                            &mut server.storage,
                        )
                        .exec()
                        .await
                    }
                    _ => bail!("expirty arg is error"),
                }
            }
            _ => bail!("set arg is error"),
        },
        b"config" => Config::new(&s[2..], &server.db_conf).exec(),
        b"keys" => Keys::new(&s[2..], &server.storage).exec().await,

        _ => bail!("cmd parse error"),
    }
}
