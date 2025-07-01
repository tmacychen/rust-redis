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
                    println!("{read_count} and buffer :{line}");
                    if read_count == 0 {
                        break;
                    }
                    if line.starts_with("*1") {
                        line.clear();
                        // read $4
                        reader.read_line(&mut line).unwrap();
                        line.clear();
                        // read command
                        reader.read_line(&mut line).unwrap();
                        if let "PING" = line.trim_end_matches('\n').trim_end_matches('\r') {
                            reader.get_mut().write(b"+PONG\r\n").unwrap();
                        } else {
                            reader.get_mut().write(b"Error:Unknown Command\n").unwrap();
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
