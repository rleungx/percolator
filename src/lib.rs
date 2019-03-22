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

// This is related to protobuf as described in `msg.proto`.
mod msg {
    include!(concat!(env!("OUT_DIR"), "/msg.rs"));
}

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// Key is a tuple (raw key, timestamp).
type Key = (Vec<u8>, u64);

enum Column {
    Write,
    Data,
    Lock,
}

#[derive(Clone, PartialEq)]
enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Clone, Default)]
struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

#[derive(Debug, Clone)]
struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone, Default)]
struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[derive(Clone, Default)]
struct TimestampOracle {
    // You definitions here if needed.
}
