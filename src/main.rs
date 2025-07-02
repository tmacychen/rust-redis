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
        println!("{line}");

        match line.trim() {
            "*1" => {
                // read $num. num is len of args\r\n
                reader.read_line(&mut line).unwrap();
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line.contains("PING") {
                    reader.get_mut().write_all(b"+PONG\r\n").unwrap();
                }
                line.clear();
            }
            "*2" => {
                // read $num. num is len of args\r\n
                reader.read_line(&mut line).unwrap();
                reader.read_line(&mut line).unwrap();

                if line.contains("ECHO") {
                    reader.read_line(&mut line).unwrap();
                    line.clear();
                    reader.read_line(&mut line).unwrap();

                    // println!("${}\r\n{}", line.trim().len(), line);
                    reader
                        .get_mut()
                        .write_all(format!("${}\r\n{}", line.trim().len(), line).as_bytes())
                        .unwrap();
                }
                line.clear();
            }
            _ => {
                reader.get_mut().write_all(b"unknown command\r\n").unwrap();
            }
        }
    }
}
