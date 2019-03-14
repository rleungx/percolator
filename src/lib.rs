#[macro_use]
extern crate fail;
#[macro_use]
extern crate prost_derive;
extern crate labcodec;
#[macro_use]
extern crate labrpc;

mod client;
mod imp;
mod service;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;

use crate::service::TSOClient;

type Key = (Vec<u8>, u64);

pub enum CF {
    Write,
    Data,
    Lock,
}

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Clone, Default)]
pub struct KvTable {
    // column write <(Vector, Timestamp), Timestamp>
    write: BTreeMap<Key, Value>,
    // column data <(Vector, Timestamp), Vector>
    data: BTreeMap<Key, Value>,
    // column lock <(Vector, Timestamp), Vector>
    lock: BTreeMap<Key, Value>,
}

#[derive(Debug, Clone)]
struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone)]
pub struct MemoryStorageTransaction {
    oracle: Arc<Mutex<TSOClient>>,
    start_ts: u64,
    data: Arc<Mutex<KvTable>>,
    writes: Vec<Write>,
}

#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
    oracle: Arc<Mutex<TSOClient>>,
    transactions: Arc<Mutex<HashMap<u64, MemoryStorageTransaction>>>,
}

impl MemoryStorage {
    pub fn new(client: TSOClient) -> Self {
        Self {
            data: Arc::new(Mutex::new(KvTable::default())),
            oracle: Arc::new(Mutex::new(client)),
            transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
