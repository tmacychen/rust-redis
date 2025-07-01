#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
};

use anyhow::Result;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(|| {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: std::net::TcpStream) {
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();

    while let Ok(read_count) = reader.read_line(&mut line) {
        if read_count == 0 {
            break;
        }
        if line.starts_with("*1") {
            line.clear();
            // read $4\r\n
            reader.read_line(&mut line).unwrap();
            line.clear();
            // read command
            reader.read_line(&mut line).unwrap();
            if line.contains("PING") {
                reader.get_mut().write_all(b"+PONG\r\n").unwrap();
            }
            line.clear();
        }
    }
}
