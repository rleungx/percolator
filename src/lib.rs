#[macro_use]
extern crate fail;
#[macro_use]
extern crate prost_derive;
extern crate labcodec;
#[macro_use]
extern crate labrpc;

mod imp;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time;

type Key = (Vec<u8>, u64);

#[derive(Clone, Default)]
pub struct KvTable {
    // column write <(Key, Timestamp), Timestamp>
    write: BTreeMap<Key, u64>,
    // column data <(Key, Timestamp), Value>
    data: BTreeMap<Key, Vec<u8>>,
    // column lock <(Key, Timestamp), Key>
    lock: BTreeMap<Key, Vec<u8>>,
}

#[derive(Debug, Clone)]
struct Write(Vec<u8>, Vec<u8>);

#[derive(Clone)]
pub struct MemoryStorageTransaction {
    oracle: Arc<Mutex<Client>>,
    start_ts: u64,
    data: Arc<Mutex<KvTable>>,
    writes: Vec<Write>,
}

#[derive(Clone)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
    oracle: Arc<Mutex<Client>>,
    transactions: Arc<Mutex<HashMap<u64, MemoryStorageTransaction>>>,
}

impl MemoryStorage {
    pub fn new(client: Client) -> Self {
        Self {
            data: Arc::new(Mutex::new(KvTable::default())),
            oracle: Arc::new(Mutex::new(client)),
            transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Timestamp {
    #[prost(uint64, tag = "1")]
    pub ts: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetTimestamp {}

service! {
    service timestamp {
        rpc get_timestamp(GetTimestamp) returns (Timestamp);
    }
}

pub use timestamp::{add_service, Client, Service};

#[derive(Clone, PartialEq, Message)]
pub struct GetRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(bytes, tag = "2")]
    pub key: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetResponse {
    #[prost(bytes, tag = "1")]
    pub value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(bytes, tag = "2")]
    pub key: Vec<u8>,
    #[prost(bytes, tag = "3")]
    pub value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetResponse {}

#[derive(Clone, PartialEq, Message)]
pub struct CommitRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct CommitResponse {
    #[prost(bool, tag = "1")]
    pub res: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct BeginRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct BeginResponse {}

service! {
    service transaction {
        rpc begin(BeginRequest) returns (BeginResponse);
        rpc get(GetRequest) returns (GetResponse);
        rpc set(SetRequest) returns (SetResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
    }
}

pub use transaction::{add_service as add_transaction_service, Client as TransactionClient};

#[derive(Clone)]
pub struct TimestampService;
