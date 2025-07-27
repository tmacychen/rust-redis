/*






*/

use dashmap::DashMap;

#[derive(Clone, Debug)]
pub enum InfoType {
    String(String),
    I64(i64),
}

// #[derive(Clone, Debug)]
// pub struct Replication {
//     info: DashMap<String, InfoType>,
// }

#[derive(Clone, Debug)]
pub struct Replication {
    info: DashMap<String, String>,
}
impl Replication {
    pub fn new() -> Self {
        let rep = Replication {
            info: DashMap::new(),
        };
        rep.info.insert("role".to_string(), "master".to_string());
        rep
    }

    pub fn get(&self, k: &str) -> Option<String> {
        match self.info.get(k) {
            Some(v) => Some(v.value().clone()),
            None => None,
        }
    }
    pub fn get_all(&self) -> Vec<(String, String)> {
        let mut ret = Vec::new();
        self.info
            .iter()
            .for_each(|e| ret.push((e.key().clone(), e.value().clone())));
        ret
    }
}
