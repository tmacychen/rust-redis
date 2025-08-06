use std::{
    fs,
    ops::DerefMut,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, MutexGuard},
};

use crate::{
    db::{Dbconf, RdbFile, RdbParser, RDB_VERSION},
    replication::{Replication, ReplicationSet},
};
use anyhow::{bail, Result};
use dashmap::DashMap;
use rand::rng;
use rand::{distr::Alphabetic, Rng};
use resp_protocol::{Array, ArrayBuilder, BulkString, SimpleString};
use tklog::{error, info};
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
    pub port: String,
    pub db_conf: Dbconf,
    pub replicaof: Option<(String, String)>,
    master_replid: String,
    master_repl_offset: u32,
    pub(crate) is_master: bool,
}

impl ServerOpt {
    pub fn new(
        port: String,
        db_conf: Dbconf,
        replicaof: Option<(String, String)>,
        is_master: bool,
    ) -> Self {
        let replid = rng()
            .sample_iter(&Alphabetic)
            .map(char::from)
            .take(40)
            .collect();
        ServerOpt {
            port: port,
            db_conf: db_conf,
            replicaof: replicaof,
            master_replid: replid,
            master_repl_offset: 0,
            is_master,
        }
    }
    pub fn get_master_replid(&self) -> String {
        self.master_replid.clone()
    }
    pub fn get_repl_offset(&self) -> u32 {
        self.master_repl_offset
    }
    pub fn change_repl_offset(&mut self, o: u32) {
        self.master_repl_offset = o;
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
            let mut stream = TcpStream::connect(format!("{}:{}", addr, port))
                .await
                .expect("connect master failed!!");

            self.ping_master(&mut stream)
                .await
                .expect("ping master failed!");
            self.repl_conf(&mut stream)
                .await
                .expect("repl conf failed!");
            self.psync(&mut stream).await.expect("psync failed!");
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

    pub async fn ping_master(&self, stream: &mut TcpStream) -> Result<()> {
        let respon_byte = ArrayBuilder::new()
            .insert(resp_protocol::RespType::BulkString(BulkString::new(
                b"PING",
            )))
            .build();
        stream.writable().await?;
        stream.write_all(&respon_byte.to_vec()).await?;
        stream.flush().await?;

        Server::get_repspon_master(stream, b"PONG").await
    }

    pub async fn repl_conf(&self, stream: &mut TcpStream) -> Result<()> {
        let mut listen_port = ArrayBuilder::new();

        listen_port.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"REPLCONF",
        )));
        listen_port.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"listening-port",
        )));
        listen_port.insert(resp_protocol::RespType::BulkString(BulkString::new(
            self.option.port.as_bytes(),
        )));

        log::debug!("wirte:listening-port!{:?}", listen_port.build());
        let mut psync = ArrayBuilder::new();

        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"REPLCONF",
        )));
        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"capa",
        )));
        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"psync2",
        )));

        stream.writable().await?;
        stream.write_all(&listen_port.build().to_vec()).await?;
        log::debug!("wirte:ncapa psync2");
        stream.write_all(&psync.build().to_vec()).await?;
        stream.flush().await?;

        Server::get_repspon_master(stream, b"OK").await
    }

    pub async fn psync(&self, stream: &mut TcpStream) -> Result<()> {
        let mut psync = ArrayBuilder::new();

        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(
            b"PSYNC",
        )));
        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(b"?")));
        psync.insert(resp_protocol::RespType::BulkString(BulkString::new(b"-1")));

        stream.writable().await?;
        log::debug!("wirte:psync");
        stream.write_all(&psync.build().to_vec()).await?;
        stream.flush().await?;

        Server::get_repspon_master(stream, b"OK").await
    }
    async fn get_repspon_master(stream: &mut TcpStream, expect: &[u8]) -> Result<()> {
        let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];
        let n = stream.read(&mut buf).await?;
        log::debug!(
            "[Master Response!] read from stream bytes num is  {}\t to string is {}",
            n,
            String::from_utf8_lossy(&buf[0..n]).to_string()
        );
        let read_response =
            SimpleString::parse(&buf, &mut 0, &buf.len()).expect("simple string parse error!");
        if read_response != SimpleString::new(expect) {
            bail!("Cant't receive correct info from master")
        }
        Ok(())
    }

    pub async fn insert_a_repl(&self, a_repl: Replication) {
        self.repl_set.lock().await.add_a_repl(a_repl);
    }

    pub async fn is_a_repl_exsits(&self, a_repl: Replication) -> bool {
        self.repl_set.lock().await.is_exsits(a_repl)
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
            let output: Vec<u8>;

            match commands::from_cmd_to_exec(l.to_vec(), arg_len, self).await {
                Ok(o) => {
                    output = o;
                }
                Err(e) => {
                    bail!("get a error from cmd{e}");
                }
            }

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
