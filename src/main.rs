use std::io::{BufRead, Cursor, ErrorKind, Read};

use tokio::{
    io::AsyncWriteExt,
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
        let mut stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_client(&mut stream).await {
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

async fn handle_client(stream: &mut TcpStream) -> Result<()> {
    let mut buf: [u8; 100] = [0; 100];
    loop {
        stream.readable().await?;

        match stream.try_read(&mut buf) {
            Ok(n) => {
                // println!("{:?} at {n} bytes", buf);
                handle_command(&buf, stream);
                break;
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

async fn handle_command(buf: &[u8], stream: &mut TcpStream) {
    let mut reader = Cursor::new(buf);
    let mut control_cmd = [0; 2];
    reader
        .read_exact(&mut control_cmd)
        .expect("read control cmd!");
    println!("{:?}", control_cmd);
    if control_cmd[0] == b'*' {
        match control_cmd[1] {
            b'1' => {
                let mut line = String::with_capacity(10);
                loop {
                    let n = reader
                        .read_line(&mut line)
                        .expect("read a line from reader");
                    if line.contains("PING") {
                        if let Err(e) = stream.write_all(b"+PONG\r\n").await {
                            eprintln!("Write client failed {:?}", e);
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
            }
            b'2' => {}
            _ => {}
        }
    }
}
//         match line.trim() {
//             "*1" => {
//                 // read $num. num is len of args\r\n
//                 reader.read_line(&mut line).unwrap();
//                 line.clear();
//                 reader.read_line(&mut line).unwrap();
//                 if line.contains("PING") {
//                     reader.get_mut().write_all(b"+PONG\r\n").unwrap();
//                 }
//                 line.clear();
//             }
//             "*2" => {
//                 // read $num. num is len of args\r\n
//                 reader.read_line(&mut line).unwrap();
//                 reader.read_line(&mut line).unwrap();

//                 if line.contains("ECHO") {
//                     reader.read_line(&mut line).unwrap();
//                     line.clear();
//                     reader.read_line(&mut line).unwrap();

//                     // println!("${}\r\n{}", line.trim().len(), line);
//                     reader
//                         .get_mut()
//                         .write_all(format!("${}\r\n{}", line.trim().len(), line).as_bytes())
//                         .unwrap();
//                 }
//                 line.clear();
//                 }
