use crate::service::{BeginRequest, CommitRequest, GetRequest, GetTimestamp, SetRequest};
use crate::service::{TSOClient, TransactionClient};

use std::thread;
use std::time::Duration;

use futures::Future;
use labrpc::*;

pub struct Client {
    txn_client: TransactionClient,
    tso_client: TSOClient,
}

const BACKOFF_TIME_MS: u64 = 100;

impl Client {
    pub fn new(
        rn: &Network,
        txn_name: &str,
        tso_name: &str,
        server_name: &str,
        tso_server_name: &str,
    ) -> Client {
        let txn_client = TransactionClient::new(rn.create_client(txn_name.to_owned()));
        rn.enable(txn_name, true);
        rn.connect(txn_name, server_name);
        let tso_client = TSOClient::new(rn.create_client(tso_name.to_owned()));
        rn.enable(tso_name, true);
        rn.connect(tso_name, tso_server_name);
        Client {
            txn_client,
            tso_client,
        }
    }

    pub fn try_get_timestamp(&self, retry: usize) -> Result<u64> {
        let mut backoff = BACKOFF_TIME_MS;
        for _i in 0..retry {
            match self.tso_client.get_timestamp(&GetTimestamp {}).wait() {
                Ok(res) => {
                    return Ok(res.ts);
                }
                Err(_) => {
                    thread::sleep(Duration::from_millis(backoff));
                    backoff *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    pub fn begin(&self, id: u64, start_ts: u64) {
        let _ = self.txn_client.begin(&BeginRequest { id, start_ts }).wait();
    }

    pub fn get(&self, id: u64, key: Vec<u8>) -> Vec<u8> {
        self.txn_client
            .get(&GetRequest { id, key })
            .wait()
            .unwrap()
            .value
    }

    pub fn set(&self, id: u64, key: Vec<u8>, value: Vec<u8>) {
        let _ = self.txn_client.set(&SetRequest { id, key, value }).wait();
    }

    pub fn commit(&self, id: u64, commit_ts: u64) -> bool {
        self.txn_client
            .commit(&CommitRequest { id, commit_ts })
            .wait()
            .unwrap()
            .res
    }
}
