// pub enum Command {
//     PING(PingCommand),
//     ECHO(EchoCommand),
//     SET(SetComand),
//     GET(GetCommand),
// }

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
pub struct SetCommand {}

impl SetCommand {
    pub fn execute() -> Vec<u8> {
        Vec::from(b"+PONG\r\n")
    }
}
pub struct GetCommand {}

impl GetCommand {
    pub fn execute() -> Vec<u8> {
        Vec::from(b"+PONG\r\n")
    }
}
