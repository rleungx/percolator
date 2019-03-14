use std::thread;
use std::time::Duration;

use crate::timestamp::*;
use crate::*;

const MAX_TIME_TO_ALIVE: u64 = Duration::from_secs(1).as_nanos() as u64;
const BACKOFF_TIME_MS: u64 = 500;

impl KvTable {
    #[inline]
    pub fn get_write(
        &self,
        key: Vec<u8>,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &u64)> {
        let range_start = match ts_start_inclusive {
            None => (key.clone(), 0),
            Some(ts) => (key.clone(), ts),
        };
        let range_end = match ts_end_inclusive {
            None => (key.clone(), std::u64::MAX),
            Some(ts) => (key.clone(), ts),
        };
        let mut r = self.write.range(range_start..=range_end);
        r.next_back()
    }

    #[inline]
    pub fn get_data(&self, key: Vec<u8>, start_ts: u64) -> Option<Vec<u8>> {
        let map_key = (key, start_ts);
        self.data.get(&map_key).map(|v| v.to_vec())
    }

    #[inline]
    pub fn get_lock(
        &self,
        key: Vec<u8>,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Vec<u8>)> {
        let range_start = match ts_start_inclusive {
            None => (key.clone(), 0),
            Some(ts) => (key.clone(), ts),
        };
        let range_end = match ts_end_inclusive {
            None => (key.clone(), std::u64::MAX),
            Some(ts) => (key.clone(), ts),
        };
        let mut r = self.lock.range(range_start..=range_end);
        r.next_back()
    }

    #[inline]
    pub fn put_write(&mut self, key: Vec<u8>, commit_ts: u64, start_ts: u64) {
        let map_key = (key, commit_ts);
        let _ = self.write.insert(map_key, start_ts);
    }

    #[inline]
    pub fn put_data(&mut self, key: Vec<u8>, start_ts: u64, value: Vec<u8>) {
        let map_key = (key, start_ts);
        let _ = self.data.insert(map_key, value);
    }

    #[inline]
    pub fn put_lock(&mut self, key: Vec<u8>, start_ts: u64, value: Vec<u8>) {
        let map_key = (key, start_ts);
        let _ = self.lock.insert(map_key, value);
    }

    #[inline]
    pub fn erase_data(&mut self, key: Vec<u8>, commit_ts: u64) {
        let l = self.data.clone();

        for (map_key, _) in l.iter() {
            if key.as_slice() == map_key.0.as_slice() && map_key.1 <= commit_ts {
                let _ = self.data.remove(&map_key);
            }
        }
    }

    #[inline]
    pub fn erase_lock(&mut self, key: Vec<u8>, commit_ts: u64) {
        let l = self.lock.clone();

        for (map_key, _) in l.iter() {
            if key.as_slice() == map_key.0.as_slice() && map_key.1 <= commit_ts {
                let _ = self.lock.remove(&map_key);
            }
        }
    }

    #[inline]
    pub fn get_uncommitted_keys(&self, ts: u64, primary: Vec<u8>) -> Vec<Key> {
        let mut keys: Vec<Key> = vec![];
        for (map_key, v) in self.lock.iter() {
            if *v == primary && map_key.1 == ts {
                keys.push((*map_key).clone());
            }
        }

        keys
    }

    #[inline]
    pub fn get_commit_ts(&self, ts: u64, primary: Vec<u8>) -> Option<u64> {
        for (map_key, v) in self.write.iter() {
            if *v == ts && map_key.0 == primary {
                return Some(map_key.1);
            }
        }

        None
    }
}

impl transaction::Service for MemoryStorage {
    fn begin(&self, req: BeginRequest) -> BeginResponse {
        let txn = MemoryStorageTransaction {
            oracle: self.oracle.clone(),
            start_ts: self
                .oracle
                .lock()
                .unwrap()
                .get_timestamp(&GetTimestamp {})
                .unwrap()
                .ts,
            data: self.data.clone(),
            writes: vec![],
        };
        self.transactions.lock().unwrap().insert(req.id, txn);

        BeginResponse {}
    }

    fn get(&self, req: GetRequest) -> GetResponse {
        let txns = self.transactions.lock().unwrap();
        let txn = txns.get(&req.id).unwrap();
        loop {
            let key = req.key.clone();
            let snapshot = txn.data.lock().unwrap().clone();

            if snapshot
                .get_lock(key.clone(), None, Some(txn.start_ts))
                .is_some()
            {
                // Check for locks that signal concurrent writes.
                txn.back_off_maybe_clean_up_lock(key.clone());
                continue;
            }

            // Find the latest write below our start timestamp.
            let (_, data_ts) = match snapshot.get_write(key.clone(), None, Some(txn.start_ts)) {
                Some(res) => res,
                None => return GetResponse { value: Vec::new() },
            };
            let v = match snapshot.get_data(key.clone(), *data_ts) {
                Some(res) => res,
                None => return GetResponse { value: Vec::new() },
            };

            return GetResponse { value: v };
        }
    }

    fn set(&self, req: SetRequest) -> SetResponse {
        let mut txns = self.transactions.lock().unwrap();
        let txn = txns.get_mut(&req.id).unwrap();
        let key = req.key;
        let value = req.value;
        txn.writes.push(Write(key, value));
        SetResponse {}
    }

    fn commit(&self, req: CommitRequest) -> CommitResponse {
        let txns = self.transactions.lock().unwrap();
        let txn = txns.get(&req.id).unwrap();
        let primary = &txn.writes[0];

        let secondaries = &txn.writes[1..];

        if !txn.prewrite(primary, primary) {
            return CommitResponse { res: false };
        }

        for w in secondaries {
            if !txn.prewrite(w, primary) {
                return CommitResponse { res: false };
            }
        }

        // Commit primary first.
        let commit_ts = txn
            .oracle
            .lock()
            .unwrap()
            .get_timestamp(&GetTimestamp {})
            .unwrap()
            .ts;
        let mut kv_data = txn.data.lock().unwrap();

        if kv_data
            .get_lock(primary.0.clone(), Some(txn.start_ts), Some(txn.start_ts))
            .is_none()
        {
            // Lock is not found.
            return CommitResponse { res: false };
        }

        kv_data.put_write(primary.0.clone(), commit_ts, txn.start_ts);
        fail_point!("commit_primary_fail", |_| { CommitResponse { res: false } });
        kv_data.erase_lock(primary.0.clone(), commit_ts);
        fail_point!("commit_secondaries_fail", |_| {
            CommitResponse { res: true }
        });

        // Second phase: write out write records for secondary cells.
        for w in secondaries {
            kv_data.put_write(w.0.clone(), commit_ts, txn.start_ts);
            kv_data.erase_lock(w.0.clone(), commit_ts);
        }

        CommitResponse { res: true }
    }
}

impl MemoryStorageTransaction {
    // Prewrite tries to lock cell w, returning false in case of conflict.
    fn prewrite(&self, w: &Write, primary: &Write) -> bool {
        let mut kv_data = self.data.lock().unwrap();

        if kv_data
            .get_write(w.0.clone(), Some(self.start_ts), None)
            .is_some()
        {
            // Abort on writes after our start timestamp ...
            return false;
        }

        if kv_data.get_lock(w.0.clone(), None, None).is_some() {
            // ... or locks at any timestamp.
            return false;
        }

        kv_data.put_data(w.0.clone(), self.start_ts, w.1.clone());
        kv_data.put_lock(w.0.clone(), self.start_ts, primary.0.clone());

        true
    }

    fn back_off_maybe_clean_up_lock(&self, key: Vec<u8>) {
        let mut kv_data = self.data.lock().unwrap();

        if let Some(r) = kv_data.get_lock(key.clone(), None, Some(self.start_ts)) {
            if self
                .oracle
                .lock()
                .unwrap()
                .get_timestamp(&GetTimestamp {})
                .unwrap()
                .ts
                - (*r.0).1
                > MAX_TIME_TO_ALIVE
            {
                let primary = (*r.1).clone();
                let ts = (*r.0).1;
                if kv_data
                    .get_lock(primary.clone(), Some(ts), Some(ts))
                    .is_some()
                {
                    let uncommitted_keys = kv_data.get_uncommitted_keys(ts, primary);

                    for k in uncommitted_keys {
                        kv_data.erase_data(k.0.clone(), ts);
                        kv_data.erase_lock(k.0.clone(), ts);
                    }
                } else {
                    let uncommitted_keys = kv_data.get_uncommitted_keys(ts, primary.clone());
                    let commit_ts = kv_data.get_commit_ts(ts, primary).unwrap();

                    for k in uncommitted_keys {
                        kv_data.put_write(k.0.clone(), commit_ts, ts);
                        kv_data.erase_lock(k.0.clone(), commit_ts);
                    }
                }
                return;
            }
        }

        thread::sleep(Duration::from_millis(BACKOFF_TIME_MS));
    }
}

impl Service for TimestampService {
    fn get_timestamp(&self, _input: GetTimestamp) -> Timestamp {
        let now = time::SystemTime::now();
        Timestamp {
            ts: now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64,
        }
    }
}
