use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

use labrpc::{RpcFuture};

const MAX_TIME_TO_ALIVE: u64 = Duration::from_millis(100).as_nanos() as u64;

impl KvTable {
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        unimplemented!()
    }

    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        unimplemented!()
    }

    #[inline]
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        unimplemented!()
    }
}

impl transaction::Service for MemoryStorage {
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        // Your code here.
        unimplemented!()
    }

    // Prewrite tries to lock cell w, returning false in case of conflict.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        // Your code here.
        unimplemented!()
    }

    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        // Your code here.
        unimplemented!()
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}

impl timestamp::Service for TimestampOracle {
    fn get_timestamp(&self, _: TimestampRequest) -> RpcFuture<TimestampResponse> {
        // Your code here.
        unimplemented!()
    }
}
