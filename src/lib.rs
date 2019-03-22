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
use std::sync::{Arc, Mutex};
use std::time;

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

impl Value {
    fn unwrap_ts(self) -> u64 {
        match self {
            Value::Timestamp(ts) => ts,
            _ => {
                panic!("Something wrong! It should be used for Timestamp");
            }
        }
    }

    fn unwrap_vec(self) -> Vec<u8> {
        match self {
            Value::Vector(val) => val,
            _ => {
                panic!("Something wrong! It should be used for Vector");
            }
        }
    }
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
struct TimestampOracle {}
