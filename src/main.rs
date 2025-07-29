use tokio::net::TcpListener;

use anyhow::{bail, Result};
use std::sync::Arc;
use tklog::{error, info, Format, LEVEL, LOG};

use clap::Parser;

use crate::server::ServerOpt;

mod commands;
mod db;
mod replication;
mod server;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// db saved dir
    #[arg(long, default_value = "")]
    dir: String,
    // db saved file name
    #[arg(long, default_value = "")]
    dbfilename: String,

    // server port
    #[arg(short, long, default_value = "6379")]
    port: String,

    // start a slave replication for the master
    #[arg(short, long, default_value = "")]
    replicaof: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    //for log
    LOG.set_console(true)
        // .set_level(LEVEL::Info)
        .set_level(LEVEL::Debug)
        .set_format(Format::LevelFlag | Format::ShortFileName)
        .set_formatter("{level}{file}:{message}\n")
        .uselog();

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    info!("Logs from your program will appear here!");
    // for args
    let args = Args::parse();

    let url = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(url).await.unwrap();

    // create a db in memory
    let mut db_conf = db::Dbconf::new();
    if !args.dir.is_empty() && !args.dbfilename.is_empty() {
        db_conf.set(args.dir.clone(), args.dbfilename.clone());
    }

    //--replicaof "localhost 6379"
    let rep: Vec<&str> = args.replicaof.split_whitespace().collect();

    let s_opt = if rep.len() < 2 || rep[1].parse::<u32>().is_err() {
        log::debug!(" replicaof's arguments parse None!!!");
        ServerOpt::new(db_conf, None)
    } else {
        ServerOpt::new(
            db_conf,
            Some((rep[0].to_lowercase(), rep[1].to_lowercase())),
        )
    };

    let server_arc = Arc::new(
        server::Server::new(s_opt)
            .await
            .expect("create server error"),
    );
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let server_clone = Arc::clone(&server_arc);
                tokio::spawn(async move {
                    if let Err(e) = server_clone.handle_client(stream).await {
                        error!("handle client error :{}", e);
                    }
                });
            }
            Err(e) => {
                error!("listener accept error: {}", e);
            }
        }
    }
}
