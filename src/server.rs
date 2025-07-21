use crate::db::{self, DataBase};
use anyhow::{bail, Result};
use resp_protocol::Array;
use std::sync::Arc;
use tklog::info;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::commands;
const BUF_SIZE: usize = 100;

// #[derive(Clone)]
// pub struct ServerOption {
//     pub dir: String,
//     pub db_filename: String,
// }

#[derive(Clone)]
pub struct Server {
    pub storage: Arc<Mutex<DataBase>>,
    pub db_conf: db::Dbconf,
}

impl Server {
    pub async fn new(db: Arc<Mutex<DataBase>>, db_conf: db::Dbconf) -> Self {
        let mut server = Server {
            storage: db,
            db_conf: db_conf,
        };
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
                "read from stream  is  {}",
                String::from_utf8_lossy(&buf[0..n]).to_string()
            );
            let read_array: Array = Array::parse(&buf, &mut 0, &n).expect("bulkString parse error");
            log::debug!("read from stream is {:?}", read_array);

            let r = read_array.to_vec();
            log::debug!("read from stream is {:?}", &r);
            let read_slice: Vec<&[u8]> = r
                .split(|c| *c == b'\r' || *c == b'\n')
                .filter(|s| !s.is_empty())
                .collect();
            // log::debug!("read from stream is {:?}", read_slice);
            log::debug!("read from stream  slice is {:?}", read_slice);

            let (f, l) = read_slice.split_first().expect("parse command error !");

            if f[0] != b'*' {
                bail!("fail to parse first command for  * ")
            }

            let arg_len: u8 = String::from_utf8_lossy(&f[1..])
                .parse()
                .expect("parse arg_len error");

            log::debug!("arg len:is {}", arg_len);

            let output = commands::from_cmd_to_exec(l.to_vec(), arg_len, self)
                .await
                .expect("exec cmd error !");

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
