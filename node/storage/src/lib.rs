use kvdb::KeyValueDB;

pub mod config;
pub mod error;
pub mod log_store;

pub use config::Config as StorageConfig;
pub use log_store::log_manager::LogManager;

pub use ethereum_types::H256;
use kvdb_memorydb::InMemory;
use kvdb_rocksdb::Database;

pub trait ZgsKeyValueDB: KeyValueDB {
    fn put(&self, col: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.put(col, key, value);
        self.write(tx)
    }

    fn puts(&self, items: Vec<(u32, Vec<u8>, Vec<u8>)>) -> std::io::Result<()> {
        let mut tx = self.transaction();
        items
            .into_iter()
            .for_each(|(col, key, val)| tx.put(col, &key, &val));
        self.write(tx)
    }

    fn delete(&self, col: u32, key: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.delete(col, key);
        self.write(tx)
    }

    fn delete_with_prefix(&self, col: u32, key_prefix: &[u8]) -> std::io::Result<()> {
        let mut tx = self.transaction();
        tx.delete_prefix(col, key_prefix);
        self.write(tx)
    }

    fn num_keys(&self, col: u32) -> std::io::Result<u64>;
}

impl ZgsKeyValueDB for Database {
    fn num_keys(&self, col: u32) -> std::io::Result<u64> {
        self.num_keys(col)
    }
}

impl ZgsKeyValueDB for InMemory {
    fn num_keys(&self, _col: u32) -> std::io::Result<u64> {
        todo!("not used")
    }
}
