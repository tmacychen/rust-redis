use dashmap::DashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
};

use anyhow::{anyhow, bail, Result};
use std::sync::Arc;

use crate::commands::{EchoCommand, PingCommand};

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //
    let map_set = Arc::new(DashMap::new());

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let map_clone = Arc::clone(&map_set);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, map_clone).await {
                        eprintln!("{e}");
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_client(
    mut stream: TcpStream,
    mut map_set: Arc<DashMap<String, String>>,
) -> Result<()> {
    let mut buf: [u8; 100] = [0; 100];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            println!("read size is 0,break loop !");
            break;
        }

        let mut reader = std::io::Cursor::new(buf);

        let mut control_cmd = [0; 4];
        std::io::Read::read_exact(&mut reader, &mut control_cmd)?;
        println!("cmd code is {}", String::from_utf8_lossy(&control_cmd));
        let arg_len: u8 = if control_cmd[1].is_ascii_alphanumeric() {
            control_cmd[1] - '0' as u8
        } else {
            bail!("get arg len error");
        };

        let cmd = read_a_line(&mut reader);

        let output = cmd_handler(&mut reader, cmd, arg_len - 1, &mut map_set)
            .await
            .expect("cmd handler err!");

        // println!("output :{}", String::from_utf8(output.clone()).unwrap());
        stream.writable().await?;
        if let Err(e) = stream.write_all(&output).await {
            eprintln!("Write client failed {:?}", e);
        }
    }

    println!(" handle client !{:?}", map_set);
    Ok(())
}
mod commands;
async fn cmd_handler(
    reader: &mut std::io::Cursor<[u8; 100]>,
    cmd: Vec<u8>,
    mut arg_len: u8,
    map_set: &mut Arc<DashMap<String, String>>,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();

    // delete the \r\n
    match String::from_utf8(cmd).unwrap().to_uppercase().trim() {
        "PING" => {
            output.extend_from_slice(PingCommand::execute().as_slice());
            // output.extend_from_slice(b"+PONG\r\n");
        }
        "ECHO" => {
            // TODO: fix
            // find error if echo args len > 10
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
            map_set.insert(k, v);
            output.extend_from_slice(b"+OK\r\n");
            println!(" cmd handler {:?}", map_set);
        }
        "GET" => {
            let k = String::from_utf8(read_a_line(reader)).expect("get k");
            loop {
                let v = map_set.try_get(&k);
                if !v.is_locked() {
                    if let Some(l) = v.try_unwrap() {
                        //l have \r\n
                        output.extend_from_slice(
                            format!("${}\r\n{}", l.len() - 2, l.to_string()).as_bytes(),
                        );
                        print!("get v :{}", l.to_string());
                    } else {
                        eprintln!("get v :none");
                        output.extend_from_slice(b"$-1\r\n");
                    }
                    break;
                } else {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
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
