// pub enum Command {
//     PING(PingCommand),
//     ECHO(EchoCommand),
//     SET(SetComand),
//     GET(GetCommand),
// }

use std::{sync::Arc, time};

use crate::db::DataBase;

pub trait Exec {
    fn execute(self) -> Vec<u8>;
}
pub struct PingCommand {}

impl PingCommand {
    pub fn execute() -> Vec<u8> {
        Vec::from(b"+PONG\r\n")
    }
}

pub struct EchoCommand {
    pub echo_back: Vec<u8>,
}

impl EchoCommand {
    pub fn new() -> EchoCommand {
        EchoCommand {
            echo_back: Vec::new(),
        }
    }
    pub fn execute(self) -> Vec<u8> {
        self.echo_back
    }
}

use std::hash::Hash;
pub struct SetCommand {}

impl SetCommand {
    // pub fn execute<K, V>(
    //     k: String,
    //     v: String,
    //     exp_str: String,
    //     db: &mut Arc<DataBase<K, V>>,
    // ) -> Vec<u8>
    // where
    //     K: Eq + Hash + Clone,
    //     V: Clone,
    // {
    //     db.kv_insert(k.clone(), v);
    //     if !exp_str.is_empty() {
    //         db.set_expiry_time(k, (exp_str, time::Instant::now()));
    //     }
    //     Vec::from(b"+OK\r\n")
    // }
}
pub struct GetCommand {}

impl GetCommand {
    pub fn execute() -> Vec<u8> {
        Vec::from(b"+PONG\r\n")
    }
}
