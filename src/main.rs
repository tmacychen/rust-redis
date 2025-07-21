use tokio::net::TcpListener;
use tokio::sync::Mutex;

use anyhow::Result;
use std::{default, sync::Arc};
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

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // create a db in memory
    let mut db = db::DataBase::new();
    let mut db_conf = db::Dbconf::new();
    if !args.dir.is_empty() && !args.dbfilename.is_empty() {
        db_conf.set(args.dir.clone(), args.dbfilename.clone());
    }

    let db_arc = Arc::new(Mutex::new(db));

    // //save arguments to db
    // if !dir.is_empty() {
    //     db.kv_insert("dir".to_lowercase(), dir);
    // }
    // if !dir_file_name.is_empty() {
    //     db.kv_insert("dirfilename".to_lowercase(), dir_file_name);
    // }
    let server = server::Server::new(db_arc, db_conf).await;

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                let mut server_clone = server.clone();
                // let db_clone = Arc::clone(&db);
                tokio::spawn(async move {
                    if let Err(e) = server_clone.handle_client(stream).await {
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

// mod commands;

// async fn cmd_handler(
//     reader: &mut std::io::Cursor<[u8; 100]>,
//     cmd: Vec<u8>,
//     mut arg_len: u8,
//     db: &mut Arc<DataBase>,
// ) -> Result<Vec<u8>> {
//     let mut output = Vec::new();

//     // delete the \r\n
//     match String::from_utf8(cmd).unwrap().to_uppercase().trim() {
//         "PING" => {
//             output.extend_from_slice(PingCommand::execute().as_slice());
//             // output.extend_from_slice(b"+PONG\r\n");
//         }
//         "ECHO" => {
//             // TODO: fix find error if echo args len > 10
//             while arg_len > 0 {
//                 let line = read_a_line(reader);
//                 // println!("line :{line} arg len is {arg_len}");
//                 let mut echo_cmd = EchoCommand::new();

//                 echo_cmd
//                     .echo_back
//                     .extend_from_slice(format!("${}\r\n", line.len() - 2).as_bytes());
//                 echo_cmd.echo_back.extend_from_slice(line.as_ref());

//                 // echo_cmd.execute();
//                 output = echo_cmd.echo_back;

//                 arg_len -= 1;
//             }
//         }
//         "SET" => {
//             // String::from_utf8(line).expect("read a line from vec to string")
//             let k = String::from_utf8(read_a_line(reader)).expect("get k");
//             let v = String::from_utf8(read_a_line(reader)).expect("get v");

//             log::debug!("arg_len is {}", arg_len);
//             let exp_str: String = if arg_len - 2 > 0 {
//                 let exp_cmd = String::from_utf8(read_a_line(reader)).expect("get expire time cmd");
//                 log::debug!("exp_cmd:{}", exp_cmd.trim());
//                 if exp_cmd.trim().to_lowercase() == "px" {
//                     String::from_utf8_lossy(read_a_line(reader).trim_ascii_end()).to_string()
//                         + " ms"
//                 } else if exp_cmd.trim().to_lowercase() == "ex" {
//                     String::from_utf8_lossy(read_a_line(reader).trim_ascii_end()).to_string() + " s"
//                 } else {
//                     "".to_string()
//                 }
//             } else {
//                 "".to_string()
//             };
//             log::debug!("k:{} v:{} exp_str:{}", &k, &v, &exp_str);
//             if !exp_str.is_empty() {
//                 db.set_expiry_time(k.clone(), (exp_str, time::Instant::now()));
//             }
//             db.kv_insert(k, v);
//             output.extend_from_slice(b"+OK\r\n");
//         }
//         "GET" => {
//             return get_cmd(reader, db);
//         }
//         "CONFIG" => {
//             return config_cmd(reader, arg_len, db);
//         }
//         "KEY" => {
//             return key_cmd(reader, arg_len, db);
//         }
//         _ => {}
//     }
//     Ok(output)
// }

// fn get_cmd(
//     reader: &mut std::io::Cursor<[u8; 100]>,
//     db: &mut Arc<DataBase<String, String>>,
// ) -> Result<Vec<u8>> {
//     let mut output = Vec::new();
//     let k = String::from_utf8(read_a_line(reader)).expect("get key panic");
//     if let Some((exp_str, t)) = db.get_expiry_time(&k) {
//         debug!("get exp_str:{}", &exp_str);
//         log::debug!("get t:{:?}", &t);
//         //example :"100 ms" or "20 s"
//         let exp_t: Vec<&str> = exp_str.split_whitespace().collect();
//         match exp_t[1] {
//             "ms" => {
//                 if let Ok(exp_t_0) = exp_t[0].parse::<u128>() {
//                     log::debug!("exp_t_0:{},t.elapsed:{}", exp_t_0, t.elapsed().as_millis());
//                     if exp_t_0 < t.elapsed().as_millis() {
//                         db.kv_delete(&k);
//                         db.del_expiry_time(&k);
//                     }
//                 } else {
//                     bail!("expire time parse error!:{}", exp_t[1])
//                 }
//             }
//             "s" => {
//                 if let Ok(exp_t_0) = exp_t[0].parse::<u64>() {
//                     if exp_t_0 < t.elapsed().as_secs() {
//                         db.kv_delete(&k);
//                         db.del_expiry_time(&k);
//                     }
//                 } else {
//                     bail!("expire time parse error!:{}", exp_t[1])
//                 }
//             }
//             _ => {
//                 bail!("args error: miss time's units ");
//             }
//         }
//     }
//     if let Some(l) = db.kv_get(&k) {
//         //l have \r\n
//         output.extend_from_slice(format!("${}\r\n{}", l.len() - 2, l.to_string()).as_bytes());
//         log::debug!("get v :{}", l.to_string());
//     } else {
//         info!("get v :none");
//         output.extend_from_slice(b"$-1\r\n");
//     }
//     Ok(output)
// }

// fn key_cmd(
//     reader: &mut std::io::Cursor<[u8; 100]>,
//     arg_len: u8,
//     db: &mut Arc<DataBase<String, String>>,
// ) -> Result<Vec<u8>> {
//     let key_arg = String::from_utf8(read_a_line(reader)).expect("read from key args");
//     log::debug!("key_arg is {}", key_arg);
//     todo!()
// }

// fn config_cmd(
//     reader: &mut std::io::Cursor<[u8; 100]>,
//     arg_len: u8,
//     db: &mut Arc<DataBase<String, String>>,
// ) -> Result<Vec<u8>> {
//     let config_sub_cmd = String::from_utf8(read_a_line(reader)).expect("get config cmd panic");
//     log::debug!("config:{} arg_len is {}", config_sub_cmd.trim(), arg_len);

//     match config_sub_cmd.trim().to_uppercase().as_str() {
//         "GET" => {
//             if arg_len - 1 > 0 {
//                 let get_arg = String::from_utf8(read_a_line(reader).trim_ascii().to_vec())
//                     .expect("get config cmd arg panic");

//                 log::debug!("config get :{}", &get_arg);
//                 if let Some(get_arg_value) = db.kv_get(&get_arg) {
//                     let mut output_array: ArrayBuilder = ArrayBuilder::new();
//                     output_array.insert(RespType::BulkString(BulkString::new(get_arg.as_bytes())));
//                     output_array.insert(RespType::BulkString(BulkString::new(
//                         get_arg_value.as_bytes(),
//                     )));

//                     let output: Array = output_array.build();
//                     log::debug!("config get :{:?}", output);
//                     // log::debug!("config get :{:?}", output.bytes().to_vec());
//                     // bytes to vec can save \r\n
//                     // arrary to vec whill time the last \r\n
//                     return Ok(output.bytes().to_vec());
//                 } else {
//                     bail!("no get{} from db", &get_arg);
//                 }
//             } else {
//                 log::debug!("config get miss arg ");
//                 bail!("config get miss arg ");
//             }
//         }
//         _ => {
//             bail!("config cmd is error");
//         }
//     }
// }

// https://lib.rs/crates/resp-protocol can parse the resp
// https://github.com/iorust/resp.git
// fn read_a_line(reader: &mut std::io::Cursor<[u8; 100]>) -> Vec<u8> {
//     let size = get_next_size(reader);
//     // +2 for \r\n
//     let mut line = vec![0u8; size + 2];
//     std::io::Read::read_exact(reader, &mut line).expect("read a line !");
//     log::debug!("line is {:?} len is {} ", line, line.len());
//     // String::from_utf8(line).expect("read a line from vec to string")
//     line
// }

// fn get_next_size(reader: &mut std::io::Cursor<[u8; 100]>) -> usize {
//     let mut cmd_size = [0; 4];
//     std::io::Read::read_exact(reader, &mut cmd_size).expect("read cmd size !");
//     let size = String::from_utf8_lossy(&cmd_size);
//     // log::debug!("cmd_size:{:?}", &cmd_size);
//     log::debug!("cmd_size:{}", size.to_string());
//     //trim and cut off '*'
//     size.trim()
//         .get(1..)
//         .unwrap()
//         .parse()
//         .expect("get size from str to u8")
// }
