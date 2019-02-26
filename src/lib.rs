#[macro_use]
extern crate fail;

mod imp;
mod rpc;
#[cfg(test)]
mod tests;

use jsonrpc_core::{self, Result};
use jsonrpc_derive::rpc;

use crate::rpc::Rpc;

pub struct StorageBuilder;

impl StorageBuilder {
    pub fn build(rpc: Rpc) -> impl Store {
        imp::MemoryStorage::new(rpc)
        // unimplemented!()
    }
}

pub struct TimeStampOracleBuilder;
impl TimeStampOracleBuilder {
    pub fn build() -> impl TimeStamp {
        imp::TimeStampOracle::new()
        // unimplemented!()
    }
}

#[rpc]
pub trait TimeStamp {
    #[rpc(name = "rpc_get_timestamp")]
    fn get_timestamp(&self) -> Result<u64>;
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
