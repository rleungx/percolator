#[macro_use]
extern crate prost_derive;
extern crate labcodec;
#[macro_use]
extern crate labrpc;

// After you finish the implementation, `#[allow(unused)]` should be removed.
#[allow(dead_code, unused)]
mod client;
#[allow(unused)]
mod imp;
mod service;
#[cfg(test)]
mod tests;

mod msg {
    include!(concat!(env!("OUT_DIR"), "/msg.rs"));
}

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

#[derive(Debug, Clone)]
struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
}
