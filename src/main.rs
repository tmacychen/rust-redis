use std::io::{BufRead, Read};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream).await {
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

async fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut buf: [u8; 100] = [0; 100];
    let mut map_set: Vec<(String, String)> = Vec::new();
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            println!("read size is 0,break loop !");
            break;
        }

        let mut reader = std::io::Cursor::new(buf);
        let mut control_cmd = [0; 4];
        std::io::Read::read_exact(&mut reader, &mut control_cmd)?;
        println!("{}", String::from_utf8_lossy(&control_cmd));
        let mut size = get_cmd_size(&mut reader);
        let mut output = Vec::new();
        let mut line = String::with_capacity(10);

        if control_cmd[0] == b'*' {
            match control_cmd[1] {
                b'1' => {
                    while size > 0 {
                        line.clear();
                        let n = reader
                            .read_line(&mut line)
                            .expect("read a line from reader");
                        // reader get last include '\r\n' so size will be -2,must use i8.
                        size = size - n as i8;
                        if line.contains("PING") {
                            output.extend_from_slice(b"+PONG\r\n");
                        }
                    }
                }
                b'2' => {
                    while size > 0 {
                        // reader get last include '\r\n' so size will be -2,must use i8.
                        let mut n = read_a_line(&mut line, &mut reader);
                        if line.contains("ECHO") {
                            let content_size = get_cmd_size(&mut reader);
                            println!("{}", content_size);

                            n = n + content_size + read_a_line(&mut line, &mut reader);

                            output.extend_from_slice(
                                format!("${}\r\n{}\r\n", content_size, line).as_bytes(),
                            );
                        }
                        size = size - n as i8;
                    }
                }
                b'3' => {
                    std::io::Read::read_to_string(&mut reader, &mut line)
                        .expect("read command to buf");
                    let mut set_cmd = line.split_ascii_whitespace().collect::<Vec<&str>>();
                    set_cmd.pop(); // pop the all 0  at last position in  buf
                    println!("{:?}", set_cmd);
                    match set_cmd[0] {
                        "SET" => {
                            map_set.push(("aaa".to_string(), "bbb".to_string()));
                            // map_set
                            //     .insert(
                            //         set_cmd.get(2).expect("get SET key").to_string(),
                            //         set_cmd.get(4).expect("get SET value").to_string(),
                            //     )
                            //     .expect("insert k and v into map");
                            output.extend_from_slice(b"+OK\r\n");
                        }
                        "GET" => {
                            let (_, v) = map_set.pop().take().unwrap();
                            println!("{v}");
                            // let v = map_set
                            //     .pop(set_cmd.get(2).expect("get GET key").to_string().as_str())
                            //     .expect("get key from map_set")
                            //     .clone();
                            output
                                .extend_from_slice(format!("${}\r\n{}\r\n", v.len(), v).as_bytes());
                        }
                        _ => {
                            println!("unknown command beside of  set and get ")
                        }
                    }
                }

                _ => {}
            }
        }

        stream.writable().await?;
        if let Err(e) = stream.write_all(&output).await {
            eprintln!("Write client failed {:?}", e);
        }
    }

    Ok(())
}

fn read_a_line(line: &mut String, reader: &mut std::io::Cursor<[u8; 100]>) -> i8 {
    line.clear();
    reader.read_line(line).expect("read a line from reader") as i8
}

fn get_cmd_size(reader: &mut std::io::Cursor<[u8; 100]>) -> i8 {
    let mut cmd_size = [0; 4];
    std::io::Read::read_exact(reader, &mut cmd_size).expect("read cmd size !");
    let size = String::from_utf8_lossy(&cmd_size);
    size.get(1..2)
        .unwrap()
        .parse()
        .expect("get size from str to u8")
}
