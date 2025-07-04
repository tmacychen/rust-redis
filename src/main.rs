use std::{
    io::{BufRead, Read},
    str::FromStr,
};

use dashmap::DashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use anyhow::{anyhow, bail, Context, Error, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //
    let mut map_set: DashMap<String, String> = DashMap::new();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let map_clone = map_set.clone();
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

async fn handle_client(mut stream: TcpStream, mut map_set: DashMap<String, String>) -> Result<()> {
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

        let output = cmd_handler(&mut reader, cmd.trim(), arg_len - 1, &mut map_set)
            .expect("cmd handler err!");

        stream.writable().await?;
        if let Err(e) = stream.write_all(&output).await {
            eprintln!("Write client failed {:?}", e);
        }

        // if control_cmd[0] == b'*' {
        //     match control_cmd[1] {
        //         b'1' => {
        //             while size > 0 {
        //                 line.clear();
        //                 let n = reader
        //                     .read_line(&mut line)
        //                     .expect("read a line from reader");
        //                 // reader get last include '\r\n' so size will be -2,must use i8.
        //                 size -= n as i8;
        //                 if line.contains("PING") {
        //                     output.extend_from_slice(b"+PONG\r\n");
        //                 }
        //             }
        //         }
        //         b'2' => {
        //             while size > 0 {
        //                 // reader get last include '\r\n' so size will be -2,must use i8.
        //                 let mut n = read_a_line(&mut line, &mut reader);
        //                 if line.contains("ECHO") {
        //                     let content_size = get_next_size(&mut reader);
        //                     println!("{}", content_size);

        //                     n = n + content_size + read_a_line(&mut line, &mut reader);

        //                     output.extend_from_slice(
        //                         format!("${}\r\n{}\r\n", content_size, line).as_bytes(),
        //                     );
        //                 }
        //                 size -= n;
        //             }
        //         }
        //         b'3' => {
        //             std::io::Read::read_to_string(&mut reader, &mut line)
        //                 .expect("read command to buf");
        //             let mut set_cmd = line.split_ascii_whitespace().collect::<Vec<&str>>();
        //             set_cmd.pop(); // pop the all 0  at last position in  buf
        //             println!("{:?}", set_cmd);
        //             match set_cmd[0] {
        //                 "SET" => {
        //                     map_set.insert(
        //                         set_cmd.get(2).expect("get SET key").to_string(),
        //                         set_cmd.get(4).expect("get SET value").to_string(),
        //                     );
        //                     output.extend_from_slice(b"+OK\r\n");
        //                 }
        //                 "GET" => {
        //                     // let v = map_set
        //                     //     .get(set_cmd.get(2).expect("get GET key").to_string().as_str())
        //                     //     .expect("get key from map_set")
        //                     //     .clone();
        //                     let v = map_set.get("aaa").expect("get hash map value").to_string();
        //                     println!("{v}");
        //                     output
        //                         .extend_from_slice(format!("${}\r\n{}\r\n", v.len(), v).as_bytes());
        //                 }
        //                 _ => {
        //                     println!("unknown command beside of  set and get ")
        //                 }
        //             }
        //         }

        //         _ => {}
        //     }
        // }
    }

    Ok(())
}

fn cmd_handler(
    reader: &mut std::io::Cursor<[u8; 100]>,
    cmd: &str,
    mut arg_len: u8,
    map_set: &mut DashMap<String, String>,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    match cmd {
        "PING" => {
            output.extend_from_slice(b"+PONG\r\n");
        }
        "ECHO" => {
            // TODO: fix
            // find error if echo args len > 10
            while arg_len > 0 {
                let content_size = get_next_size(reader);
                if content_size <= 0 {
                    return Err(anyhow!("content size is nagtive :{content_size}"));
                }
                // println!("in echo :{content_size}");

                let line = read_a_line(reader);
                // println!("line :{line} arg len is {arg_len}");

                output.extend_from_slice(format!("${}\r\n{}", content_size, line).as_bytes());
                arg_len -= 1;
            }
        }
        "SET" => {
            let k = read_a_line(reader);
            let v = read_a_line(reader);
            map_set.insert(k, v);
            output.extend_from_slice(b"+OK\r\n");
        }
        "GET" => {
            let k = read_a_line(reader);
            let v = map_set.try_get(&k);
            if !v.is_locked() {
                let l = v.unwrap().to_string();
                // println!("l and l's len {l},{}", l.len());
                // output.extend_from_slice(format!("${}\r\n{}", l.len(), l).as_bytes());
            } else {
                output.extend_from_slice(b"$-1\r\n");
            }
        }
        _ => {}
    }
    Ok(output)
}
fn read_a_line(reader: &mut std::io::Cursor<[u8; 100]>) -> String {
    let size = get_next_size(reader);
    // +2 for \r\n
    let mut line = vec![0u8; size + 2];
    std::io::Read::read_exact(reader, &mut line).expect("read a line !");
    // print!("line is {:?} len is {} ", line, line.len());
    String::from_utf8(line).expect("read a line from vec to string")
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
