#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();

                while let Ok(read_count) = reader.read_line(&mut line) {
                    if read_count == 0 {
                        break;
                    }
                    println!("{read_count}:{line}");
                    if line.starts_with("*1") {
                        line.clear();
                        // read $4\r\n
                        reader.read_line(&mut line).unwrap();
                        println!("$4:{line}");
                        line.clear();
                        // read command
                        reader.read_line(&mut line).unwrap();
                        println!("cmmand : {line}");
                        if line.contains("PING") {
                            reader.get_mut().write_all(b"+PONG\r\n").unwrap();
                        }
                        line.clear();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
