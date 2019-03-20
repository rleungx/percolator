use crate::service::{TSOClient, TransactionClient};
use labrpc::*;

#[derive(Clone)]
pub struct Client {
    // Your definitions here.
}

const BACKOFF_TIME_MS: u64 = 100;
const RETRY_TIMES: usize = 3;

impl Client {
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {}
    }

    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        unimplemented!()
    }

    pub fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
