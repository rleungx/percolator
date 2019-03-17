use crate::msg::{CommitRequest, GetRequest, GetTimestamp, PrewriteRequest};
use crate::service::{TSOClient, TransactionClient};
use crate::Write;

use std::thread;
use std::time::Duration;

use futures::Future;
use labrpc::*;

#[derive(Default)]
pub struct Transaction {
    start_ts: u64,
    writes: Vec<Write>,
}

pub struct Client {
    txn_client: TransactionClient,
    tso_client: TSOClient,
    txn: Transaction,
}

const BACKOFF_TIME_MS: u64 = 100;
const RETRY_TIMES: usize = 3;

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
            txn: Transaction {
                ..Default::default()
            },
        }
    }

    pub fn try_get_timestamp(&self) -> Result<u64> {
        let mut backoff = BACKOFF_TIME_MS;
        for _i in 0..RETRY_TIMES {
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

    pub fn begin(&mut self) {
        let start_ts = match self.try_get_timestamp() {
            Ok(ts) => ts,
            Err(Error::Timeout) => {
                println!("get timestamp timeout");
                return;
            }
            Err(_) => panic!("unexpected behavior"),
        };
        self.txn = Transaction {
            start_ts,
            writes: vec![],
        };
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let mut backoff = BACKOFF_TIME_MS;
        for _i in 0..RETRY_TIMES {
            match self
                .txn_client
                .get(&GetRequest {
                    start_ts: self.txn.start_ts,
                    key: key.clone(),
                })
                .wait()
            {
                Ok(res) => {
                    return Ok(res.value);
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

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.txn.writes.push(Write(key, value));
    }

    pub fn commit(&self) -> bool {
        let primary = &self.txn.writes[0];
        let secondaries = &self.txn.writes[1..];

        if self
            .txn_client
            .prewrite(&PrewriteRequest {
                start_ts: self.txn.start_ts,
                write: Some(crate::msg::Write {
                    key: primary.0.clone(),
                    value: primary.1.clone(),
                }),
                primary: Some(crate::msg::Write {
                    key: primary.0.clone(),
                    value: primary.1.clone(),
                }),
            })
            .wait().is_err()
        {
            return false;
        }

        for w in secondaries {
            if self
                .txn_client
                .prewrite(&PrewriteRequest {
                    start_ts: self.txn.start_ts,
                    write: Some(crate::msg::Write {
                        key: w.0.clone(),
                        value: w.1.clone(),
                    }),
                    primary: Some(crate::msg::Write {
                        key: primary.0.clone(),
                        value: primary.1.clone(),
                    }),
                })
                .wait().is_err()
            {
                return false;
            }
        }

        let commit_ts = match self.try_get_timestamp() {
            Ok(ts) => ts,
            Err(Error::Timeout) => {
                println!("get timestamp timeout");
                return false;
            }
            Err(_) => panic!("unexpected behavior"),
        };
        // Commit primary first.
        if self
            .txn_client
            .commit(&CommitRequest {
                is_primary: true,
                start_ts: self.txn.start_ts,
                commit_ts,
                write: Some(crate::msg::Write {
                    key: primary.0.clone(),
                    value: primary.1.clone(),
                }),
            })
            .wait().is_err()
        {
            return false;
        }

        // Second phase: write out write records for secondary cells.
        for w in secondaries {
            if self
                .txn_client
                .commit(&CommitRequest {
                    is_primary: false,
                    start_ts: self.txn.start_ts,
                    commit_ts,
                    write: Some(crate::msg::Write {
                        key: w.0.clone(),
                        value: w.1.clone(),
                    }),
                })
                .wait().is_err()
            {
                return false;
            }
        }

        true
    }
}
