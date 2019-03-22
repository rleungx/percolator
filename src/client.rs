use crate::msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest};
use crate::service::{TSOClient, TransactionClient};
use crate::Write;

use std::time::Duration;

use futures::Future;
use futures_timer::Delay;
use labrpc::*;

#[derive(Clone, Default)]
struct Transaction {
    start_ts: u64,
    writes: Vec<Write>,
}

#[derive(Clone)]
pub struct Client {
    tso_client: TSOClient,
    txn_client: TransactionClient,
    txn: Transaction,
}

const BACKOFF_TIME_MS: u64 = 100;
const RETRY_TIMES: usize = 3;

impl Client {
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        Client {
            tso_client,
            txn_client,
            txn: Transaction {
                ..Default::default()
            },
        }
    }

    pub fn get_timestamp(&self) -> Result<u64> {
        let mut backoff = BACKOFF_TIME_MS;
        for _i in 0..RETRY_TIMES {
            match self.tso_client.get_timestamp(&TimestampRequest {}).wait() {
                Ok(res) => {
                    return Ok(res.ts);
                }
                Err(_) => {
                    Delay::new(Duration::from_millis(backoff)).wait().unwrap();
                    backoff *= 2;
                    continue;
                }
            }
        }
        Err(Error::Timeout)
    }

    pub fn begin(&mut self) {
        let start_ts = match self.get_timestamp() {
            Ok(ts) => ts,
            Err(Error::Timeout) => {
                println!("get timestamp timeout");
                return;
            }
            _ => panic!("unexpected behavior"),
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
                    Delay::new(Duration::from_millis(backoff)).wait().unwrap();
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

    pub fn commit(&self) -> Result<bool> {
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
            .wait()
            .is_err()
        {
            return Ok(false);
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
                .wait()
                .is_err()
            {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;
        // Commit primary first.
        match self
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
            .wait()
        {
            Ok(_) => {}
            Err(Error::Other(e)) => {
                if e == "resphook" {
                    return Err(Error::Other("resphook".to_owned()));
                } else {
                    return Ok(false);
                }
            }
            Err(_) => return Ok(false),
        }

        // Second phase: write out write records for secondary cells.
        for w in secondaries {
            let _ = self
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
                .wait();
        }

        Ok(true)
    }
}
