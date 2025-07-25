use std::{fs, path::PathBuf, sync::Arc};

use crate::{
    db::{self, Dbconf, RdbFile, RdbParser, RDB_VERSION},
    replication::Replication,
};
use anyhow::{bail, Result};
use resp_protocol::Array;
use tklog::info;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::commands;
const BUF_SIZE: usize = 100;

#[derive(Clone)]
pub struct Server {
    pub storage: Arc<Mutex<RdbFile>>,
    pub db_conf: Dbconf,
    pub rep: Arc<Mutex<Replication>>,
}

impl Server {
    pub async fn new(db_conf: db::Dbconf) -> Result<Self> {
        let mut server: Server;

        let mut file_path = PathBuf::from(db_conf.get_dir());
        if db_conf.get_dir() != "" && !file_path.exists() {
            fs::create_dir(file_path.as_path()).expect("creat redis dir error");
        }
        if db_conf.get_db_filename() != "" {
            file_path.push(db_conf.get_db_filename());
        }

        //if file
        if file_path.is_file() {
            let mut rdbfile_reader = RdbParser::new(File::open(file_path.as_path()).await?);
            server = Server {
                storage: Arc::new(Mutex::new(
                    rdbfile_reader.parse().await.expect("rdb_file parse error"),
                )),
                db_conf: db_conf,
                rep: Arc::new(Mutex::new(Replication::new())),
            };
        } else {
            server = Server {
                storage: Arc::new(Mutex::new(RdbFile::new(RDB_VERSION))),
                db_conf: db_conf,
                rep: Arc::new(Mutex::new(Replication::new())),
            };
        }

        server.init().await;
        Ok(server)
    }
    pub async fn init(&mut self) {
        log::info!("server init executed !!");
    }

    pub async fn handle_client(&self, mut stream: TcpStream) -> Result<()> {
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
