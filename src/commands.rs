use std::time;

use crate::{
    db::{DataBase, Dbconf, ValueType},
    server::Server,
};
use anyhow::{bail, Result};
use resp_protocol::{ArrayBuilder, BulkString, RespType, SimpleString};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct Ping;

#[derive(Clone, Debug)]
pub struct Echo(BulkString);

#[derive(Clone)]
pub struct Get(String, Arc<Mutex<DataBase>>);

#[derive(Clone)]
pub struct Set(String, ValueType, Arc<Mutex<DataBase>>);

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

impl Get {
    async fn exec(&self) -> Result<Vec<u8>> {
        let db = self.1.lock().await;
        if let Some((value, expire)) = db.get(&self.0) {
            match expire {
                Some((expire_str, instant_time)) => {
                    let exp_t: Vec<&str> = expire_str.split_whitespace().collect();
                    match exp_t[1] {
                        "ms" => {
                            if let Ok(exp_t_0) = exp_t[0].parse::<u128>() {
                                log::debug!(
                                    "exp_t_0:{},t.elapsed:{}",
                                    exp_t_0,
                                    instant_time.elapsed().as_millis()
                                );
                                if exp_t_0 < instant_time.elapsed().as_millis() {
                                    db.delete(&self.0);
                                    Ok(SimpleString::new(b"-1").bytes().to_vec())
                                } else {
                                    Ok(BulkString::new(value.as_bytes()).bytes().to_vec())
                                }
                            } else {
                                bail!("expire time parse error!:{}", exp_t[1])
                            }
                        }
                        "s" => {
                            if let Ok(exp_t_0) = exp_t[0].parse::<u64>() {
                                if exp_t_0 < instant_time.elapsed().as_secs() {
                                    db.delete(&self.0);
                                    Ok(SimpleString::new(b"-1").bytes().to_vec())
                                } else {
                                    Ok(BulkString::new(value.as_bytes()).bytes().to_vec())
                                }
                            } else {
                                bail!("expire time parse error!:{}", exp_t[1])
                            }
                        }
                        _ => {
                            bail!("args error: miss time's units ");
                        }
                    }
                }
                None => Ok(BulkString::new(value.as_bytes()).bytes().to_vec()),
            }
        } else {
            Ok(SimpleString::new(b"-1").bytes().to_vec())
        }
    }
}
impl Set {
    async fn exec(&self) -> Result<Vec<u8>> {
        let db = self.2.lock().await;
        if db.insert(self.0.clone(), self.1.clone()).is_some() {
            Ok(SimpleString::new(b"OK").bytes().to_vec())
        } else {
            Ok(SimpleString::new(b"-1").bytes().to_vec())
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
                _ => Ok(SimpleString::new(b"-1").bytes().to_vec()),
            },
            _ => bail!("unknown config sub cmd "),
        }
    }
}

pub async fn from_cmd_to_exec(s: Vec<&[u8]>, arg_len: u8, server: &Server) -> Result<Vec<u8>> {
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
                Arc::clone(&server.storage),
            )
            .exec()
            .await
        }
        b"set" => match arg_len {
            3 => {
                crate::commands::Set(
                    String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                    (
                        String::from_utf8(s[5].to_vec()).expect("convert get arg to string"),
                        None,
                    ),
                    Arc::clone(&server.storage),
                )
                .exec()
                .await
            }
            5 => {
                let time_str = String::from_utf8(s[9].to_vec()).expect("conver time to str ");
                log::debug!(" set time is {}", time_str);

                match s[7] {
                    b"px" => {
                        crate::commands::Set(
                            String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                            (
                                String::from_utf8(s[5].to_vec())
                                    .expect("convert get arg to string"),
                                Some((time_str + " ms", time::Instant::now())),
                            ),
                            Arc::clone(&server.storage),
                        )
                        .exec()
                        .await
                    }
                    b"ex" => {
                        crate::commands::Set(
                            String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                            (
                                String::from_utf8(s[5].to_vec())
                                    .expect("convert get arg to string"),
                                Some((time_str + " s", time::Instant::now())),
                            ),
                            Arc::clone(&server.storage),
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

        _ => bail!("cmd parse error"),
    }
}
