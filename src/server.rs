use std::{fs, path::PathBuf};

use crate::db::{self, Dbconf, RdbFile, RdbParser, RDB_VERSION};
use anyhow::{bail, Result};
use resp_protocol::Array;
use tklog::info;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::commands;
const BUF_SIZE: usize = 100;

#[derive(Clone)]
pub struct Server {
    pub storage: RdbFile,
    pub db_conf: Dbconf,
}

impl Server {
    pub async fn new(db_conf: db::Dbconf) -> Result<Self> {
        let mut server: Server;
        let mut file_path = PathBuf::from(db_conf.get_dir());
        if !file_path.exists() {
            fs::create_dir(file_path.as_path()).expect("creat redis dir error");
        }
        file_path.push(db_conf.get_db_filename());

        //if file exists
        if file_path.exists() {
            let mut rdbfile_reader = RdbParser::new(File::open(file_path.as_path()).await?);

            server = Server {
                storage: rdbfile_reader.parse().await.expect("rdb_file parse error"),
                db_conf: db_conf,
            };
        } else {
            File::create(file_path.as_path()).await?;
            server = Server {
                storage: RdbFile::new(RDB_VERSION),
                db_conf: db_conf,
            };
        }

        server.init().await;
        Ok(server)
    }
    pub async fn init(&mut self) {
        log::info!("server init executed !!");
    }

    pub async fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                info!("read size is 0, [A client connection CLOSED !] !");
                break;
            };

            log::debug!(
                "[A Client connected !] read from stream bytes num is  {}\nto string is {}",
                n,
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
