use crate::db::DataBase;
use anyhow::{bail, Result};
use bytes::Buf;
use resp_protocol::{Array, ArrayBuilder, BulkString, RespType, SimpleString};
use std::{io::BufRead, sync::Arc};
use tklog::info;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::commands::Command;
const BUF_SIZE: usize = 100;

#[derive(Clone)]
pub struct Server {
    pub storage: Arc<DataBase>,
}

impl Server {
    pub async fn new(db: Arc<DataBase>) -> Self {
        let mut server = Server { storage: db };
        server.init().await;
        server
    }
    pub async fn init(&mut self) {}

    pub async fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                info!("read size is 0,break loop !");
                break;
            }

            log::debug!("read from stream bytes num is  {}", n);
            log::debug!(
                "read from stream bytes num is  {}",
                String::from_utf8_lossy(&buf[0..n]).to_string()
            );
            let read_array: Array = Array::parse(&buf, &mut 0, &n).expect("bulkString parse error");
            log::debug!("read from stream is {:?}", read_array);

            let r = read_array.to_vec();
            let read_slice: Vec<&[u8]> = r
                .split(|c| *c == b'\r' || *c == b'\n')
                .filter(|s| !s.is_empty())
                .collect();
            // log::debug!("read from stream is {:?}", read_slice);
            log::debug!("read from stream  slice is {:?}", read_slice);

            // std::io::Read::read_exact(&mut reader, &mut control_cmd)?;
            // let arg_len: u8 = if control_cmd[1].is_ascii_alphanumeric() {
            //     control_cmd[1] - '0' as u8
            // } else {
            //     bail!("get arg len error");
            // };

            // log::debug!("cmd arg len is{}", &arg_len);
            // let cmd = read_a_line(&mut reader);
            //
            let (f, l) = read_slice.split_first().expect("parse command error !");

            if f[0] != b'*' {
                bail!("fail to parse first command for  * ")
            }

            let arg_len: u8 = String::from_utf8_lossy(&f[1..])
                .parse()
                .expect("parse arg_len error");

            log::debug!("arg len:is {}", arg_len);

            let cmd = Command::from(l.into()).expect("get cmd error !");
            let output = cmd.exec().await.expect("cmd exec error!");

            log::debug!("output is ready to write back Vec:{:?}", &output);
            log::debug!(
                "output is ready to write back:{:?}",
                String::from_utf8_lossy(&output)
            );

            println!("output :{}", String::from_utf8(output.clone()).unwrap());

            stream.writable().await?;
            if let Err(e) = stream.write_all(&output).await {
                bail!("Write client failed {:?}", e);
            }
        }
        Ok(())
    }
}
