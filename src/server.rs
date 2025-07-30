use std::{fs, path::PathBuf, sync::Arc};

use crate::{
    db::{Dbconf, RdbFile, RdbParser, RDB_VERSION},
    replication::ReplicationSet,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
use rand::rng;
use rand::{distr::Alphabetic, Rng};
use resp_protocol::{Array, ArrayBuilder, SimpleString};
use tklog::info;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

use crate::commands;
const BUF_SIZE: usize = 100;

#[derive(Clone, Debug)]
pub struct ServerOpt {
    pub db_conf: Dbconf,
    pub replicaof: Option<(String, String)>,
    master_replid: String,
    master_repl_offset: u32,
    pub(crate) is_master: bool,
}

impl ServerOpt {
    pub fn new(db_conf: Dbconf, replicaof: Option<(String, String)>, is_master: bool) -> Self {
        let replid = rng()
            .sample_iter(&Alphabetic)
            .map(char::from)
            .take(40)
            .collect();
        ServerOpt {
            db_conf: db_conf,
            replicaof: replicaof,
            master_replid: replid,
            master_repl_offset: 0,
            is_master,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Server {
    pub storage: Arc<Mutex<RdbFile>>,
    pub option: ServerOpt,
    pub repl_set: Arc<Mutex<ReplicationSet>>,
    info: Arc<Mutex<DashMap<String, DashMap<String, String>>>>,
}

impl Server {
    pub async fn new(conf: ServerOpt) -> Result<Self> {
        let mut server: Server;

        let mut file_path = PathBuf::from(conf.db_conf.get_dir());
        if conf.db_conf.get_dir() != "" && !file_path.exists() {
            fs::create_dir(file_path.as_path()).expect("creat redis dir error");
        }
        if conf.db_conf.get_db_filename() != "" {
            file_path.push(conf.db_conf.get_db_filename());
        }

        let ser_info: DashMap<String, DashMap<String, String>> = DashMap::new();

        let rep_v: DashMap<String, String> = DashMap::new();
        if conf.replicaof.is_some() {
            rep_v.insert("role".to_string(), "slave".to_string());
        } else {
            rep_v.insert("role".to_string(), "master".to_string());
        }

        rep_v.insert("master_replid".to_string(), conf.master_replid.clone());
        rep_v.insert(
            "master_repl_offset".to_string(),
            format!("{}", conf.master_repl_offset),
        );

        ser_info.insert("replication".to_string(), rep_v);

        log::debug!("server info is {:?}", ser_info);

        //parse storage file
        let storage = if file_path.is_file() {
            let mut rdbfile_reader = RdbParser::new(File::open(file_path.as_path()).await?);
            Arc::new(Mutex::new(
                rdbfile_reader.parse().await.expect("rdb_file parse error"),
            ))
        } else {
            Arc::new(Mutex::new(RdbFile::new(RDB_VERSION)))
        };

        server = Server {
            storage: storage,
            option: conf,
            repl_set: Arc::new(Mutex::new(ReplicationSet::new())),
            info: Arc::new(Mutex::new(ser_info)),
        };

        server.init().await;

        Ok(server)
    }
    pub async fn init(&mut self) {
        if self.is_slave() {
            let (addr, port) = self.option.replicaof.as_ref().unwrap();
            let stream = TcpStream::connect(format!("{}:{}", addr, port))
                .await
                .expect("connect master failed!!");

            self.ping_master(stream).await.expect("ping master failed!");
        }
        log::info!("server init has finished!!");
    }
    pub async fn get_a_info(&self, k: &str) -> DashMap<String, String> {
        self.info
            .lock()
            .await
            .get(k)
            .expect("get server info error")
            // .value()
            .clone()
    }

    pub async fn get_all_info(&self) -> DashMap<String, DashMap<String, String>> {
        self.info.lock().await.to_owned()
    }

    pub async fn ping_master(&self, mut stream: TcpStream) -> Result<()> {
        let respon_byte = ArrayBuilder::new()
            .insert(resp_protocol::RespType::SimpleString(SimpleString::new(
                b"PING",
            )))
            .build();

        stream.writable().await?;
        stream.write_all(&respon_byte.bytes().to_vec()).await?;

        let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];
        let n = stream.read(&mut buf).await?;
        log::debug!(
            "[Master Response!] read from stream bytes num is  {}\nto string is {}",
            n,
            String::from_utf8_lossy(&buf[0..n]).to_string()
        );
        let read_array: SimpleString =
            SimpleString::parse(&buf, &mut 0, &n).expect("bulkString parse error");

        if read_array != SimpleString::new(b"PONG") {
            bail!("Cant't receive a PONG")
        }

        Ok(())
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

            stream.writable().await?;
            if let Err(e) = stream.write_all(&output).await {
                bail!("Write client failed {:?}", e);
            }
        }
        Ok(())
    }
    pub fn is_slave(&self) -> bool {
        !self.option.is_master
    }
}
