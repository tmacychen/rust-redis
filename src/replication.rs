/*






*/

use std::net::TcpStream;

#[derive(Clone, Debug)]
pub struct Replication {
    pub port: String,
}

#[derive(Clone, Debug)]
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
