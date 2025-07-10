use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use anyhow::{bail, Result};
use std::sync::Arc;
use std::time;
use tklog::{debug, error, info, Format, LEVEL, LOG};

use crate::{
    commands::{EchoCommand, PingCommand},
    db::DataBase,
};
mod db;

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    info!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    LOG.set_console(true)
        // .set_level(LEVEL::Debug)
        .set_level(LEVEL::Info)
        .set_format(Format::LevelFlag | Format::ShortFileName)
        .set_formatter("{level}{file}:{message}\n")
        .uselog();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // create a db in memory
    let db = Arc::new(db::DataBase::<String, String>::new());

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let map_clone = Arc::clone(&db);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, map_clone).await {
                        error!("handle client error :{}", e);
                    }
                });
            }
            Err(e) => {
                error!("error: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, mut db: Arc<DataBase<String, String>>) -> Result<()> {
    let mut buf: [u8; 100] = [0; 100];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            info!("read size is 0,break loop !");
            break;
        }

        let mut reader = std::io::Cursor::new(buf);

        let mut control_cmd = [0; 4];
        std::io::Read::read_exact(&mut reader, &mut control_cmd)?;
        debug!("cmd code is", String::from_utf8_lossy(&control_cmd));
        let arg_len: u8 = if control_cmd[1].is_ascii_alphanumeric() {
            control_cmd[1] - '0' as u8
        } else {
            bail!("get arg len error");
        };

        let cmd = read_a_line(&mut reader);

        let output = cmd_handler(&mut reader, cmd, arg_len - 1, &mut db)
            .await
            .expect("cmd handler err!");

        // println!("output :{}", String::from_utf8(output.clone()).unwrap());
        stream.writable().await?;
        if let Err(e) = stream.write_all(&output).await {
            error!("Write client failed {:?}", e);
        }
    }

    // println!(" handle client !");
    Ok(())
}

mod commands;
async fn cmd_handler(
    reader: &mut std::io::Cursor<[u8; 100]>,
    cmd: Vec<u8>,
    mut arg_len: u8,
    db: &mut Arc<DataBase<String, String>>,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();

    // delete the \r\n
    match String::from_utf8(cmd).unwrap().to_uppercase().trim() {
        "PING" => {
            output.extend_from_slice(PingCommand::execute().as_slice());
            // output.extend_from_slice(b"+PONG\r\n");
        }
        "ECHO" => {
            // TODO: fix find error if echo args len > 10
            while arg_len > 0 {
                let line = read_a_line(reader);
                // println!("line :{line} arg len is {arg_len}");
                let mut echo_cmd = EchoCommand::new();

                echo_cmd
                    .echo_back
                    .extend_from_slice(format!("${}\r\n", line.len() - 2).as_bytes());
                echo_cmd.echo_back.extend_from_slice(line.as_ref());

                // echo_cmd.execute();
                output = echo_cmd.echo_back;

                arg_len -= 1;
            }
        }
        "SET" => {
            // String::from_utf8(line).expect("read a line from vec to string")
            let k = String::from_utf8(read_a_line(reader)).expect("get k");
            let v = String::from_utf8(read_a_line(reader)).expect("get v");

            debug!("arg_len is ", arg_len);
            let exp_str: String = if arg_len - 2 > 0 {
                let exp_cmd = String::from_utf8(read_a_line(reader)).expect("get expire time cmd");
                debug!("exp_cmd:", exp_cmd.trim());
                if exp_cmd.trim().to_lowercase() == "px" {
                    String::from_utf8_lossy(read_a_line(reader).trim_ascii_end()).to_string()
                        + " ms"
                } else if exp_cmd.trim().to_lowercase() == "ex" {
                    String::from_utf8_lossy(read_a_line(reader).trim_ascii_end()).to_string() + " s"
                } else {
                    "".to_string()
                }
            } else {
                "".to_string()
            };
            log::debug!("k:{} v:{} exp_str:{}", &k, &v, &exp_str);
            if !exp_str.is_empty() {
                db.set_expiry_time(k.clone(), (exp_str, time::Instant::now()));
            }
            db.kv_insert(k, v);
            output.extend_from_slice(b"+OK\r\n");
        }
        "GET" => {
            let k = String::from_utf8(read_a_line(reader)).expect("get k");
            if let Some((exp_str, t)) = db.get_expiry_time(&k) {
                debug!("get exp_str:{}", &exp_str);
                log::debug!("get t:{:?}", &t);
                //example :"100 ms" or "20 s"
                let exp_t: Vec<&str> = exp_str.split_whitespace().collect();
                match exp_t[1] {
                    "ms" => {
                        if let Ok(exp_t_0) = exp_t[0].parse::<u128>() {
                            debug!("exp_t_0:", exp_t_0, "t.elapsed:", t.elapsed().as_millis());
                            if exp_t_0 < t.elapsed().as_millis() {
                                db.kv_delete(&k);
                                db.del_expiry_time(&k);
                            }
                        } else {
                            bail!("expire time parse error!:{}", exp_t[1])
                        }
                    }
                    "s" => {
                        if let Ok(exp_t_0) = exp_t[0].parse::<u64>() {
                            if exp_t_0 < t.elapsed().as_secs() {
                                db.kv_delete(&k);
                                db.del_expiry_time(&k);
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
            if let Some(l) = db.kv_get(&k) {
                //l have \r\n
                output
                    .extend_from_slice(format!("${}\r\n{}", l.len() - 2, l.to_string()).as_bytes());
                debug!("get v :", l.to_string());
            } else {
                info!("get v :none");
                output.extend_from_slice(b"$-1\r\n");
            }
        }
        _ => {}
    }
    Ok(output)
}
fn read_a_line(reader: &mut std::io::Cursor<[u8; 100]>) -> Vec<u8> {
    let size = get_next_size(reader);
    // +2 for \r\n
    let mut line = vec![0u8; size + 2];
    std::io::Read::read_exact(reader, &mut line).expect("read a line !");
    // println!("line is {:?} len is {} ", line, line.len());
    // String::from_utf8(line).expect("read a line from vec to string")
    line
}

fn get_next_size(reader: &mut std::io::Cursor<[u8; 100]>) -> usize {
    let mut cmd_size = [0; 4];
    std::io::Read::read_exact(reader, &mut cmd_size).expect("read cmd size !");
    let size = String::from_utf8_lossy(&cmd_size);
    size.get(1..2)
        .unwrap()
        .parse()
        .expect("get size from str to u8")
}
