/*






*/

use std::sync::Arc;

use tokio::{net::TcpStream, sync::Mutex};

#[derive(Debug)]
pub struct Replication {
    pub stream: Arc<Mutex<TcpStream>>,
    pub port: String,
}

#[derive(Debug)]
pub struct ReplicationSet {
    repls: Vec<Replication>,
}
impl ReplicationSet {
    pub fn new() -> Self {
        ReplicationSet { repls: Vec::new() }
    }
    pub fn add_a_repl(&mut self, a_repl: Replication) {
        self.repls.push(a_repl);
    }

    pub fn is_exsits(&self, a_repl: Replication) -> bool {
        self.repls.iter().any(|r| r.port == a_repl.port)
    }
    pub fn is_empty(&self) -> bool {
        self.repls.len() == 0
    }
}
