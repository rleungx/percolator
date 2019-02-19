use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time;

#[derive(Clone, Default)]
pub struct KvTable {
    // column write <(Key, Timestamp), Timestamp>
    write: BTreeMap<(Vec<u8>, u64), u64>,
    // column data <(Key, Timestamp), Value>
    data: BTreeMap<(Vec<u8>, u64), Vec<u8>>,
    // column lock <Key, Timestamp>
    lock: BTreeMap<Vec<u8>, u64>,
}

impl KvTable {
    #[inline]
    pub fn get_write(
        &self,
        key: Vec<u8>,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&(Vec<u8>, u64), &u64)> {
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
    pub fn get_lock(&self, key: Vec<u8>) -> Option<u64> {
        self.lock.get(key.as_slice()).cloned()
    }

    #[inline]
    pub fn put_data(&mut self, key: Vec<u8>, start_ts: u64, value: Vec<u8>) {
        let map_key = (key, start_ts);
        let _ = self.data.insert(map_key, value);
    }

    #[inline]
    pub fn put_lock(&mut self, key: Vec<u8>, start_ts: u64) {
        let _ = self.lock.insert(key, start_ts);
    }

    #[inline]
    pub fn put_write(&mut self, key: Vec<u8>, commit_ts: u64, start_ts: u64) {
        let map_key = (key, commit_ts);
        let _ = self.write.insert(map_key, start_ts);
    }

    #[inline]
    pub fn erase_lock(&mut self, key: Vec<u8>) {
        let _ = self.lock.remove(&key);
    }
}

#[derive(Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(KvTable::default())),
        }
    }
}

impl crate::Store for MemoryStorage {
    type Transaction = MemoryStorageTransaction;

    fn begin(&self) -> MemoryStorageTransaction {
        MemoryStorageTransaction {
            start_ts: get_timestamp(),
            data: self.data.clone(),
            snapshot: self.data.lock().unwrap().clone(),
            writes: vec![],
        }
    }
}

#[derive(Debug)]
struct Write(Vec<u8>, Vec<u8>);

pub struct MemoryStorageTransaction {
    start_ts: u64,
    data: Arc<Mutex<KvTable>>,
    snapshot: KvTable,
    writes: Vec<Write>,
}

impl crate::Transaction for MemoryStorageTransaction {
    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        loop {
            if let Some(ts) = self.snapshot.get_lock(key.clone()) {
                if ts < self.start_ts {
                    continue;
                }
            }
            // Check for locks that signal concurrent writes.
            // TODO: BackoffAndMaybeCleanupLock

            // Find the latest write below our start timestamp.
            let (_, data_ts) = self
                .snapshot
                .get_write(key.clone(), None, Some(self.start_ts))?;
                
            let value = self.snapshot.get_data(key.clone(), *data_ts);
            return value;
        }
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push(Write(key, value));
    }

    fn commit(self) -> bool {
        let primary = &self.writes[0];
        let secondaries = &self.writes[1..];

        if !self.prewrite(primary, primary) {
            return false;
        }
        for w in secondaries {
            if !self.prewrite(w, primary) {
                return false;
            }
        }

        // Commit primary first.
        let commit_ts = get_timestamp();
        let mut kv_data = self.data.lock().unwrap();

        if let Some(lock_ts) = kv_data.get_lock(primary.0.clone()) {
            if lock_ts != self.start_ts {
                // Lock is not found
                return false;
            }
        } else {
            // Lock is not found
            return false;
        }
        kv_data.put_write(primary.0.clone(), commit_ts, self.start_ts);
        kv_data.erase_lock(primary.0.clone());

        // Second phase: write out write records for secondary cells.
        for w in secondaries {
            kv_data.put_write(w.0.clone(), commit_ts, self.start_ts);
            kv_data.erase_lock(w.0.clone());
        }

        true
    }
}

impl MemoryStorageTransaction {
    // Prewrite tries to lock cell w, returning false in case of conflict.
    fn prewrite(&self, w: &Write, primary: &Write) -> bool {
        let mut kv_data = self.data.lock().unwrap();
        let latest_write = kv_data.get_write(w.0.clone(), Some(self.start_ts), None);
        if latest_write.is_some() {
            let (k, _) = latest_write.unwrap();
            if (*k).1 >= self.start_ts {
                // Abort on writes after our start timestamp ...
                return false;
            }
        }
        if let Some(lock_ts) = kv_data.get_lock(w.0.clone()) {
            if lock_ts != self.start_ts {
                // ... or locks at any timestamp.
                return false;
            }
        }

        kv_data.put_data(w.0.clone(), self.start_ts, w.1.clone());
        kv_data.put_lock(primary.0.clone(), self.start_ts);

        true
    }
}

// FIXME: Need a TSO for concurrently getting timestamps.
pub fn get_timestamp() -> u64 {
    let now = time::SystemTime::now();
    now.duration_since(time::UNIX_EPOCH).expect("").as_nanos() as u64
}
