#[macro_use]
extern crate fail;

mod imp;
#[cfg(test)]
mod tests;

pub struct TestStorageBuilder;

impl TestStorageBuilder {
    pub fn build() -> impl Store {
        imp::MemoryStorage::new()
        // unimplemented!()
    }
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
