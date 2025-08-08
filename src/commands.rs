use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    db::{Dbconf, Expiry, KeyValue, RdbFile, RedisValue, DB_NUM},
    replication::Replication,
    server::Server,
};
use anyhow::{bail, Result};
use log::error;
use resp_protocol::{ArrayBuilder, BulkString, Error, RespType, SimpleString, NULL_BULK_STRING};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

#[derive(Clone, Debug)]
pub struct Ping;

#[derive(Clone, Debug)]
pub struct Echo(BulkString);

pub struct Get<'a>(String, &'a Server);

pub struct Set<'a>(String, KeyValue, &'a Server);

pub struct Keys<'a>(&'a [&'a [u8]], Arc<Mutex<RdbFile>>);

pub struct Repl<'a>(&'a [&'a [u8]], &'a Server, Arc<Mutex<TcpStream>>);

impl Ping {
    fn exec(&self) -> Result<Vec<u8>> {
        // back to simplestring
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
        let mut db = self.1.storage.lock().await;

        if let Some(value) = db.get(DB_NUM, &self.0).await {
            match value.expiry {
                Some(exp_t) => match exp_t {
                    Expiry::Milliseconds(t) => {
                        let now_millis = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("now time get error")
                            .as_millis();
                        log::debug!("timestamp:{:?} ms vs time now :{:?} ms", t, now_millis);
                        if (t as u128) < now_millis {
                            log::debug!("delete a key {}", db.delete(0, &self.0).await);
                            Ok(NULL_BULK_STRING.bytes().to_vec())
                        } else {
                            get_value_from_redis_type(&value.value)
                        }
                    }
                    Expiry::Seconds(t) => {
                        let now_secs = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("now time get error")
                            .as_secs();
                        log::debug!("timestamp:{:?} ms vs time now :{:?} ms", t, now_secs);
                        if (t as u64) < now_secs {
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
            .storage
            .lock()
            .await
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
    pub fn new(cmd: &'a [&[u8]], rdb_file: Arc<Mutex<RdbFile>>) -> Self {
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
            if let Some(keys) = self.1.lock().await.keys(DB_NUM).await {
                for k in keys {
                    ret_array.insert(RespType::BulkString(BulkString::new(k.as_bytes())));
                }
                Ok(ret_array.build().bytes().to_vec())
            } else {
                Ok(BulkString::new(b"-1").bytes().to_vec())
            }
        }
    }
}
impl<'a> Repl<'a> {
    pub fn new(cmd: &'a [&[u8]], s: &'a Server, stream: Arc<Mutex<TcpStream>>) -> Self {
        Repl(cmd, s, stream)
    }

    async fn exec(&self) -> Result<Vec<u8>> {
        log::debug!("repl conf request is {:?}", &self.0);

        match self.0[1].to_ascii_lowercase().as_slice() {
            b"listening-port" => {
                self.1
                    .insert_a_repl(Replication {
                        stream: self.2.clone(),
                        port: String::from_utf8_lossy(self.0[3]).to_string(),
                    })
                    .await;
                Ok(SimpleString::new(b"OK").bytes().to_vec())
            }
            b"capa" => {
                if self.1.repl_set.lock().await.is_empty() {
                    Ok(NULL_BULK_STRING.bytes().to_vec())
                } else {
                    Ok(SimpleString::new(b"OK").bytes().to_vec())
                }
            }
            _ => bail!("unknown config sub cmd "),
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

#[derive(Clone, Debug)]
pub struct Info<'a> {
    key: Option<String>,
    server: &'a Server,
}

impl<'a> Info<'a> {
    pub fn new(k: Option<String>, server: &'a Server) -> Self {
        Info {
            key: k,
            server: server,
        }
    }

    async fn exec(&self) -> Result<Vec<u8>> {
        let mut all = String::new();
        match &self.key {
            Some(k) => {
                let v_hashmap = self.server.get_a_info(k).await;
                v_hashmap
                    .iter()
                    .for_each(|e| all.push_str(format!("{}:{}\r\n", e.key(), e.value()).as_str()));
            }
            //inter for all keys
            None => {
                let mut section = String::new();
                for e in self.server.get_all_info().await.iter() {
                    section.clear();
                    e.value().iter().for_each(|e_e| {
                        section.push_str(format!("{}:{}\r\n", e_e.key(), e_e.value()).as_str())
                    });
                    all.push_str(format!("# {}\r\n{}", e.key(), section).as_str());
                }
            }
        }
        if all.len() == 0 {
            Ok(NULL_BULK_STRING.bytes().to_vec())
        } else {
            Ok(BulkString::new(all.as_bytes()).bytes().to_vec())
        }
    }
}
pub async fn from_cmd_to_exec(
    s: Vec<&[u8]>,
    arg_len: u8,
    stream_arc: Arc<Mutex<TcpStream>>,
    server: &Server,
) -> Result<()> {
    log::debug!("get s:{:?}", s);
    let output = match s[1].to_ascii_lowercase().as_slice() {
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
                &server,
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
                    &server,
                )
                .exec()
                .await
            }
            5 => {
                let time_num = String::from_utf8(s[9].to_vec())
                    .expect("conver time to str ")
                    .parse::<u64>()
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
                                expiry: Some(Expiry::Milliseconds(
                                    SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("get now timestamp error")
                                        .as_millis() as u64
                                        + time_num,
                                )),
                            },
                            &server,
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
                                expiry: Some(Expiry::Seconds(
                                    (SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("get now timestamp error")
                                        .as_secs()
                                        + time_num) as u32,
                                )),
                            },
                            &server,
                        )
                        .exec()
                        .await
                    }
                    _ => bail!("expirty arg is error"),
                }
            }
            _ => {
                error!("set arg is error");
                Ok(BulkString::new(b"-1").bytes().to_vec())
            }
        },
        b"config" => Config::new(&s[2..], &server.option.db_conf).exec(),
        b"keys" => Keys::new(&s[2..], Arc::clone(&server.storage)).exec().await,
        b"info" => match arg_len {
            2 => Info::new(None, &server).exec().await,
            3 => {
                Info::new(Some(String::from_utf8_lossy(&s[2]).to_uppercase()), &server)
                    .exec()
                    .await
            }
            _ => bail!("info args number error!"),
        },
        b"replconf" => Repl::new(&s[2..], &server, stream_arc.clone()).exec().await,
        b"psync" => {
            log::debug!("pysync is {:?}", &s[2..]);

            match s[3].to_ascii_lowercase().as_slice() {
                b"?" => {
                    if s[5] == b"-1" {
                        let mut ret = Vec::new();
                        ret.extend(
                            SimpleString::new(
                                format!(
                                    "FULLRESYNC {} {}",
                                    server.option.get_master_replid(),
                                    server.option.get_repl_offset()
                                )
                                .as_bytes(),
                            )
                            .bytes()
                            .to_vec(),
                        );

                        let rdb_bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").expect("hex decode error");
                        ret.extend(BulkString::new(rdb_bytes.as_slice()).bytes().to_vec());
                        //for pop the last  two bytes "\r\n"
                        ret.pop();
                        ret.pop();
                        Ok(ret)
                    } else {
                        Ok(SimpleString::new(b"-1").bytes().to_vec())
                    }
                }
                _ => bail!("match psync error!"),
            }
        }

        _ => bail!("cmd parse error"),
    };

    match output {
        Ok(out) => {
            log::debug!("output is ready to write back Vec:{:?}", &out);
            log::debug!(
                "output is ready to write back:{:?}",
                String::from_utf8_lossy(&out)
            );
            stream_arc.lock().await.writable().await?;
            stream_arc.lock().await.write_all(&out).await?;
            Ok(())
        }
        Err(e) => bail!("{e}"),
    }
}
