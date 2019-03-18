#[macro_use]
extern crate prost_derive;
extern crate labcodec;
#[macro_use]
extern crate labrpc;

#[allow(dead_code)]
mod client;
mod imp;
mod service;
#[cfg(test)]
mod tests;

mod msg {
    include!(concat!(env!("OUT_DIR"), "/msg.rs"));
}

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;

type Key = (Vec<u8>, u64);

pub enum Column {
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
    start_ts: u64,
    data: Arc<Mutex<KvTable>>,
    writes: Vec<Write>,
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
    transactions: Arc<Mutex<HashMap<u64, MemoryStorageTransaction>>>,
}
