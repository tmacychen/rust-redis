/*






*/

use std::net::TcpStream;

#[derive(Debug)]
pub struct ReplicationSet {
    repl_streams: Vec<TcpStream>,
}
impl ReplicationSet {
    pub fn new() -> Self {
        ReplicationSet {
            repl_streams: Vec::new(),
        }
    }
    pub fn add_stream(&mut self, steam: TcpStream) {
        self.repl_streams.push(steam);
    }
}
