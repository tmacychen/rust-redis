use std::time;

use crate::db::ValueType;
use anyhow::{bail, Result};
use resp_protocol::{BulkString, SimpleString};

#[derive(Clone, Debug)]
pub struct Ping;
#[derive(Clone, Debug)]
pub struct Echo(BulkString);
#[derive(Clone, Debug)]
pub struct Get(String);
#[derive(Clone, Debug)]
pub struct Set(String, ValueType);

pub async fn from_to_exec(s: Vec<&[u8]>, arg_len: u8) -> Result<Vec<u8>> {
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
            )
            .exec()
        }
        b"set" => match arg_len {
            3 => crate::commands::Set(
                String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                (
                    String::from_utf8(s[5].to_vec()).expect("convert get arg to string"),
                    None,
                ),
            )
            .exec(),
            5 => {
                let time_str = String::from_utf8(s[9].to_vec()).expect("conver time to str ");
                log::debug!(" set time is {}", time_str);

                match s[7] {
                    b"px" => crate::commands::Set(
                        String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                        (
                            String::from_utf8(s[5].to_vec()).expect("convert get arg to string"),
                            Some((time_str + "ms", time::Instant::now())),
                        ),
                    )
                    .exec(),
                    b"ex" => crate::commands::Set(
                        String::from_utf8(s[3].to_vec()).expect("convert get arg to string"),
                        (
                            String::from_utf8(s[5].to_vec()).expect("convert get arg to string"),
                            Some((time_str + "s", time::Instant::now())),
                        ),
                    )
                    .exec(),
                    _ => bail!("expirty arg is error"),
                }
            }
            _ => bail!("set arg is error"),
        },
        _ => bail!("cmd parse error"),
    }
}

trait Exec {
    fn exec(&self) -> Result<Vec<u8>>;
}

impl Exec for Ping {
    fn exec(&self) -> Result<Vec<u8>> {
        Ok(SimpleString::new(b"PONG").bytes().to_vec())
    }
}
impl Exec for Echo {
    fn exec(&self) -> Result<Vec<u8>> {
        Ok(self.0.bytes().to_vec())
    }
}

impl Exec for Get {
    fn exec(&self) -> Result<Vec<u8>> {
        todo!()
    }
}
impl Exec for Set {
    fn exec(&self) -> Result<Vec<u8>> {
        todo!()
    }
}
