use std::thread;
use std::time::Duration;

use crate::service::*;
use crate::*;

use futures::Future;
use labrpc::RpcFuture;

const MAX_TIME_TO_ALIVE: u64 = Duration::from_secs(1).as_nanos() as u64;
const BACKOFF_TIME_MS: u64 = 500;

impl KvTable {
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        cf: CF,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        let range_start = match ts_start_inclusive {
            None => (key.clone(), 0),
            Some(ts) => (key.clone(), ts),
        };
        let range_end = match ts_end_inclusive {
            None => (key.clone(), std::u64::MAX),
            Some(ts) => (key.clone(), ts),
        };
        match cf {
            CF::Write => {
                let mut r = self.write.range(range_start..=range_end);
                r.next_back()
            }
            CF::Data => {
                let mut r = self.data.range(range_start..=range_end);
                r.next_back()
            }
            CF::Lock => {
                let mut r = self.lock.range(range_start..=range_end);
                r.next_back()
            }
        }
    }

    #[inline]
    fn write(&mut self, key: Vec<u8>, cf: CF, ts: u64, value: Value) {
        let map_key = (key, ts);
        match cf {
            CF::Write => {
                let _ = self.write.insert(map_key, value);
            }
            CF::Data => {
                let _ = self.data.insert(map_key, value);
            }
            CF::Lock => {
                let _ = self.lock.insert(map_key, value);
            }
        }
    }

    #[inline]
    fn erase(&mut self, key: Vec<u8>, cf: CF, commit_ts: u64) {
        match cf {
            CF::Data => {
                let l = self.data.clone();
                for (map_key, _) in l.iter() {
                    if key.as_slice() == map_key.0.as_slice() && map_key.1 <= commit_ts {
                        let _ = self.data.remove(&map_key);
                    }
                }
            }
            CF::Lock => {
                let l = self.lock.clone();
                for (map_key, _) in l.iter() {
                    if key.as_slice() == map_key.0.as_slice() && map_key.1 <= commit_ts {
                        let _ = self.lock.remove(&map_key);
                    }
                }
            }
            _ => {}
        }
    }

    #[inline]
    pub fn get_uncommitted_keys(&self, ts: u64, primary: Vec<u8>) -> Vec<Key> {
        let mut keys: Vec<Key> = vec![];
        for (map_key, v) in self.lock.iter() {
            if (*v).clone().unwrap_vec() == primary && map_key.1 == ts {
                keys.push((*map_key).clone());
            }
        }

        keys
    }

    #[inline]
    pub fn get_commit_ts(&self, ts: u64, primary: Vec<u8>) -> Option<u64> {
        for (map_key, v) in self.write.iter() {
            if (*v).clone().unwrap_ts() == ts && map_key.0 == primary {
                return Some(map_key.1);
            }
        }

        None
    }
}

impl transaction::Service for MemoryStorage {
    fn begin(&self, req: BeginRequest) -> RpcFuture<BeginResponse> {
        let txn = MemoryStorageTransaction {
            oracle: self.oracle.clone(),
            start_ts: req.start_ts,
            data: self.data.clone(),
            writes: vec![],
        };
        self.transactions.lock().unwrap().insert(req.id, txn);

        Box::new(futures::future::result(Ok(BeginResponse {})))
    }

    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        let txns = self.transactions.lock().unwrap();
        let txn = txns.get(&req.id).unwrap();
        loop {
            let key = req.key.clone();
            let snapshot = txn.data.lock().unwrap().clone();

            if snapshot
                .read(key.clone(), CF::Lock, None, Some(txn.start_ts))
                .is_some()
            {
                // Check for locks that signal concurrent writes.
                txn.back_off_maybe_clean_up_lock(key.clone());
                continue;
            }

            // Find the latest write below our start timestamp.
            let data_ts = match snapshot.read(key.clone(), CF::Write, None, Some(txn.start_ts)) {
                Some(res) => (*(res.1)).clone().unwrap_ts(),
                None => {
                    return Box::new(futures::future::result(Ok(GetResponse {
                        value: Vec::new(),
                    })))
                }
            };
            let v = match snapshot.read(key.clone(), CF::Data, Some(data_ts), Some(data_ts)) {
                Some(res) => (*(res.1)).clone().unwrap_vec(),
                None => {
                    return Box::new(futures::future::result(Ok(GetResponse {
                        value: Vec::new(),
                    })))
                }
            };

            return Box::new(futures::future::result(Ok(GetResponse { value: v })));
        }
    }

    fn set(&self, req: SetRequest) -> RpcFuture<SetResponse> {
        let mut txns = self.transactions.lock().unwrap();
        let txn = txns.get_mut(&req.id).unwrap();
        let key = req.key;
        let value = req.value;
        txn.writes.push(Write(key, value));
        Box::new(futures::future::result(Ok(SetResponse {})))
    }

    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        let txns = self.transactions.lock().unwrap();
        let txn = txns.get(&req.id).unwrap();
        let primary = &txn.writes[0];

        let secondaries = &txn.writes[1..];

        if !txn.prewrite(primary, primary) {
            return Box::new(futures::future::result(Ok(CommitResponse { res: false })));
        }

        for w in secondaries {
            if !txn.prewrite(w, primary) {
                return Box::new(futures::future::result(Ok(CommitResponse { res: false })));
            }
        }

        // Commit primary first.
        let commit_ts = req.commit_ts;
        let mut kv_data = txn.data.lock().unwrap();

        if kv_data
            .read(
                primary.0.clone(),
                CF::Lock,
                Some(txn.start_ts),
                Some(txn.start_ts),
            )
            .is_none()
        {
            // Lock is not found.
            return Box::new(futures::future::result(Ok(CommitResponse { res: false })));
        }

        kv_data.write(
            primary.0.clone(),
            CF::Write,
            commit_ts,
            Value::Timestamp(txn.start_ts),
        );
        fail_point!("commit_primary_fail", |_| {
            Box::new(futures::future::result(Ok(CommitResponse { res: false })))
        });
        kv_data.erase(primary.0.clone(), CF::Lock, commit_ts);
        fail_point!("commit_secondaries_fail", |_| {
            Box::new(futures::future::result(Ok(CommitResponse { res: true })))
        });

        // Second phase: write out write records for secondary cells.
        for w in secondaries {
            kv_data.write(
                w.0.clone(),
                CF::Write,
                commit_ts,
                Value::Timestamp(txn.start_ts),
            );
            kv_data.erase(w.0.clone(), CF::Lock, commit_ts);
        }

        Box::new(futures::future::result(Ok(CommitResponse { res: true })))
    }
}

impl MemoryStorageTransaction {
    // Prewrite tries to lock cell w, returning false in case of conflict.
    fn prewrite(&self, w: &Write, primary: &Write) -> bool {
        let mut kv_data = self.data.lock().unwrap();

        if kv_data
            .read(w.0.clone(), CF::Write, Some(self.start_ts), None)
            .is_some()
        {
            // Abort on writes after our start timestamp ...
            return false;
        }

        if kv_data.read(w.0.clone(), CF::Lock, None, None).is_some() {
            // ... or locks at any timestamp.
            return false;
        }

        kv_data.write(
            w.0.clone(),
            CF::Data,
            self.start_ts,
            Value::Vector(w.1.clone()),
        );
        kv_data.write(
            w.0.clone(),
            CF::Lock,
            self.start_ts,
            Value::Vector(primary.0.clone()),
        );

        true
    }

    fn back_off_maybe_clean_up_lock(&self, key: Vec<u8>) {
        let mut kv_data = self.data.lock().unwrap();

        if let Some(r) = kv_data.read(key.clone(), CF::Lock, None, Some(self.start_ts)) {
            let response = self
                .oracle
                .lock()
                .unwrap()
                .get_timestamp(&GetTimestamp {})
                .wait()
                .unwrap();
            if response.ts - (*r.0).1 > MAX_TIME_TO_ALIVE {
                let primary = (*r.1).clone().unwrap_vec().clone();
                let ts = (*r.0).1;
                if kv_data
                    .read(primary.clone(), CF::Lock, Some(ts), Some(ts))
                    .is_some()
                {
                    let uncommitted_keys = kv_data.get_uncommitted_keys(ts, primary);

                    for k in uncommitted_keys {
                        kv_data.erase(k.0.clone(), CF::Data, ts);
                        kv_data.erase(k.0.clone(), CF::Lock, ts);
                    }
                } else {
                    let uncommitted_keys = kv_data.get_uncommitted_keys(ts, primary.clone());
                    let commit_ts = kv_data.get_commit_ts(ts, primary).unwrap();

                    for k in uncommitted_keys {
                        kv_data.write(k.0.clone(), CF::Write, commit_ts, Value::Timestamp(ts));
                        kv_data.erase(k.0.clone(), CF::Lock, commit_ts);
                    }
                }
                return;
            }
        }

        thread::sleep(Duration::from_millis(BACKOFF_TIME_MS));
    }
}

impl Service for TimestampService {
    fn get_timestamp(&self, _input: GetTimestamp) -> RpcFuture<Timestamp> {
        let now = time::SystemTime::now();
        let ts = Timestamp {
            ts: now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64,
        };
        Box::new(futures::future::result(Ok(ts)))
    }
}

impl Value {
    fn unwrap_ts(self) -> u64 {
        match self {
            Value::Timestamp(ts) => ts,
            _ => {
                panic!("something wrong!");
            }
        }
    }

    fn unwrap_vec(self) -> Vec<u8> {
        match self {
            Value::Vector(val) => val,
            _ => {
                panic!("something wrong!");
            }
        }
    }
}
