use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

use labrpc::{Error, RpcFuture};

const MAX_TIME_TO_ALIVE: u64 = Duration::from_secs(1).as_nanos() as u64;

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
    fn get(&self, req: GetRequest) -> RpcFuture<GetResponse> {
        let key = req.key.clone();
        let snapshot = self.data.lock().unwrap().clone();

        if snapshot
            .read(key.clone(), CF::Lock, None, Some(req.start_ts))
            .is_some()
        {
            // Check for locks that signal concurrent writes.
            self.back_off_maybe_clean_up_lock(req.start_ts, key.clone());
            return Box::new(futures::future::result(Err(Error::Other(
                "Backoff".to_string(),
            ))));
        }

        // Find the latest write below our start timestamp.
        let data_ts = match snapshot.read(key.clone(), CF::Write, None, Some(req.start_ts)) {
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

        Box::new(futures::future::result(Ok(GetResponse { value: v })))
    }

    // Prewrite tries to lock cell w, returning false in case of conflict.
    fn prewrite(&self, req: PrewriteRequest) -> RpcFuture<PrewriteResponse> {
        let mut kv_data = self.data.lock().unwrap();

        if kv_data
            .read(
                req.write.as_ref().unwrap().key.clone(),
                CF::Write,
                Some(req.start_ts),
                None,
            )
            .is_some()
        {
            // Abort on writes after our start timestamp ...
            return Box::new(futures::future::result(Err(Error::Other(
                "write conflict".to_string(),
            ))));
        }

        if kv_data
            .read(
                req.write.as_ref().unwrap().key.clone(),
                CF::Lock,
                None,
                None,
            )
            .is_some()
        {
            // ... or locks at any timestamp.
            return Box::new(futures::future::result(Err(Error::Other(
                "key has already locked".to_string(),
            ))));
        }

        kv_data.write(
            req.write.as_ref().unwrap().key.clone(),
            CF::Data,
            req.start_ts,
            Value::Vector(req.write.as_ref().unwrap().value.clone()),
        );
        kv_data.write(
            req.write.as_ref().unwrap().key.clone(),
            CF::Lock,
            req.start_ts,
            Value::Vector(req.primary.unwrap().key.clone()),
        );

        Box::new(futures::future::result(Ok(PrewriteResponse {})))
    }

    fn commit(&self, req: CommitRequest) -> RpcFuture<CommitResponse> {
        let mut kv_data = self.data.lock().unwrap();
            if req.is_primary && kv_data
                .read(
                    req.write.as_ref().unwrap().key.clone(),
                    CF::Lock,
                    Some(req.start_ts),
                    Some(req.start_ts),
                )
                .is_none()
            {
                // Lock is not found.
                return Box::new(futures::future::result(Err(Error::Other(
                    "lock is not found".to_string(),
                ))));
            }

        kv_data.write(
            req.write.as_ref().unwrap().key.clone(),
            CF::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );
        kv_data.erase(
            req.write.as_ref().unwrap().key.clone(),
            CF::Lock,
            req.commit_ts,
        );

        Box::new(futures::future::result(Ok(CommitResponse {})))
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut kv_data = self.data.lock().unwrap();

        if let Some(r) = kv_data.read(key.clone(), CF::Lock, None, Some(start_ts)) {
            let now = time::SystemTime::now();
            let current_ts = now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64;
            if current_ts - (*r.0).1 > MAX_TIME_TO_ALIVE {
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
