/*






*/

use std::sync::Arc;

use tokio::{net::TcpStream, sync::Mutex};

#[derive(Clone, Debug)]
pub struct Replication {
    pub stream: Arc<Mutex<TcpStream>>,
    pub port: String,
}

#[derive(Clone, Debug)]
pub struct ReplicationSet {
    repls: Vec<Replication>,
    pub ready: bool,
}
impl ReplicationSet {
    pub fn new() -> Self {
        ReplicationSet {
            repls: Vec::new(),
            ready: false,
        }
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
    pub fn get_repls(&self) -> &[Replication] {
        self.repls.as_slice()
    }
    pub fn is_ready(&self) -> bool {
        self.ready
    }
    pub fn set_ready(&mut self, flag: bool) {
        self.ready = flag;
    }
}
