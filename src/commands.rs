/*











*/
use crate::db::ValueType;
use anyhow::{bail, Result};

#[derive(Clone, Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(String, ValueType),
    Unknow,
}

impl Command {
    pub fn from(s: Vec<&[u8]>) -> Result<Self> {
        Ok(match s[0] {
            b"ping" => Command::Ping,
            _ => Command::Unknow,
        })
    }
    pub async fn exec(&self) -> Result<Vec<u8>> {
        let output = Vec::new();
        Ok(output)
    }
}
