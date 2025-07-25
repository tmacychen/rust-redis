use tokio::net::TcpListener;

use anyhow::Result;
use std::sync::Arc;
use tklog::{error, info, Format, LEVEL, LOG};

use clap::Parser;

mod commands;
mod db;
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
    #[arg(long, default_value = "6379")]
    port: String,
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

    let server_arc = Arc::new(
        server::Server::new(db_conf)
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
