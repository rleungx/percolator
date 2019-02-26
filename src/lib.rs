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

use std::time;

pub struct StorageBuilder;

impl StorageBuilder {
    pub fn build(client: Client) -> impl Store {
        imp::MemoryStorage::new(client)
        // unimplemented!()
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

#[derive(Clone)]
pub struct TimestampService;

impl Service for TimestampService {
    fn get_timestamp(&self, _input: GetTimestamp) -> Timestamp {
        let now = time::SystemTime::now();
        Timestamp {
            ts: now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64,
        }
    }
}

pub trait Store {
    type Transaction: Transaction;

    fn begin(&self) -> Self::Transaction;
}

pub trait Transaction {
    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);

    fn commit(self) -> bool;
}
