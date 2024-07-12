#[macro_use]
extern crate tracing;

use anyhow::bail;
use shared_types::{
    Chunk, ChunkArray, ChunkArrayWithProof, DataRoot, FlowProof, FlowRangeProof, Transaction,
};
use ssz::{Decode, Encode};
use std::sync::Arc;
use storage::{error, error::Result, log_store::Store as LogStore, H256};
use task_executor::TaskExecutor;
use tokio::sync::oneshot;

pub use storage::config::ShardConfig;
use storage::log_store::config::ConfigurableExt;
use storage::log_store::{MineLoadChunk, SealAnswer, SealTask};

/// The name of the worker tokio tasks.
const WORKER_TASK_NAME: &str = "async_storage_worker";

macro_rules! delegate {
    (fn $name:tt($($v:ident: $t:ty),*)) => {
        delegate!($name($($v: $t),*) -> ());
    };

    (fn $name:tt($($v:ident: $t:ty),*) -> $ret:ty) => {
        pub async fn $name(&self, $($v: $t),*) -> $ret {
            self.spawn(move |store| store.$name($($v),*)).await
        }
    };
}

#[derive(Clone)]
pub struct Store {
    /// Log and transaction storage.
    store: Arc<dyn LogStore>,

    /// Tokio executor for spawning worker tasks.
    executor: TaskExecutor,
}

impl Store {
    pub fn new(store: Arc<dyn LogStore>, executor: TaskExecutor) -> Self {
        Store { store, executor }
    }

    delegate!(fn check_tx_completed(tx_seq: u64) -> Result<bool>);
    delegate!(fn get_chunk_by_tx_and_index(tx_seq: u64, index: usize) -> Result<Option<Chunk>>);
    delegate!(fn get_chunks_by_tx_and_index_range(tx_seq: u64, index_start: usize, index_end: usize) -> Result<Option<ChunkArray>>);
    delegate!(fn get_chunks_with_proof_by_tx_and_index_range(tx_seq: u64, index_start: usize, index_end: usize, merkle_tx_seq: Option<u64>) -> Result<Option<ChunkArrayWithProof>>);
    delegate!(fn get_tx_by_seq_number(seq: u64) -> Result<Option<Transaction>>);
    delegate!(fn put_chunks(tx_seq: u64, chunks: ChunkArray) -> Result<()>);
    delegate!(fn put_chunks_with_tx_hash(tx_seq: u64, tx_hash: H256, chunks: ChunkArray, maybe_file_proof: Option<FlowProof>) -> Result<bool>);
    delegate!(fn get_chunk_by_flow_index(index: u64, length: u64) -> Result<Option<ChunkArray>>);
    delegate!(fn finalize_tx(tx_seq: u64) -> Result<()>);
    delegate!(fn finalize_tx_with_hash(tx_seq: u64, tx_hash: H256) -> Result<bool>);
    delegate!(fn get_proof_at_root(root: Option<DataRoot>, index: u64, length: u64) -> Result<FlowRangeProof>);
    delegate!(fn get_context() -> Result<(DataRoot, u64)>);

    pub async fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> Result<Option<u64>> {
        let root = *data_root;
        self.spawn(move |store| store.get_tx_seq_by_data_root(&root))
            .await
    }

    pub async fn get_tx_by_data_root(&self, data_root: &DataRoot) -> Result<Option<Transaction>> {
        let root = *data_root;
        self.spawn(move |store| store.get_tx_by_data_root(&root))
            .await
    }

    pub async fn get_config_decoded<K: AsRef<[u8]> + Send + Sync, T: Decode + Send + 'static>(
        &self,
        key: &K,
    ) -> Result<Option<T>> {
        let key = key.as_ref().to_vec();
        self.spawn(move |store| store.get_config_decoded(&key))
            .await
    }

    pub async fn set_config_encoded<K: AsRef<[u8]> + Send + Sync, T: Encode + Send + Sync>(
        &self,
        key: &K,
        value: &T,
    ) -> anyhow::Result<()> {
        let key = key.as_ref().to_vec();
        let value = value.as_ssz_bytes();
        self.spawn(move |store| store.set_config(&key, &value))
            .await
    }

    pub async fn pull_seal_chunk(
        &self,
        seal_index_max: usize,
    ) -> anyhow::Result<Option<Vec<SealTask>>> {
        self.spawn(move |store| store.flow().pull_seal_chunk(seal_index_max))
            .await
    }

    pub async fn submit_seal_result(&self, answers: Vec<SealAnswer>) -> anyhow::Result<()> {
        self.spawn(move |store| store.flow().submit_seal_result(answers))
            .await
    }

    pub async fn load_sealed_data(&self, chunk_index: u64) -> Result<Option<MineLoadChunk>> {
        self.spawn(move |store| store.flow().load_sealed_data(chunk_index))
            .await
    }

    pub async fn get_num_entries(&self) -> Result<u64> {
        self.spawn(move |store| store.flow().get_num_entries())
            .await
    }

    pub async fn remove_chunks_batch(&self, batch_list: &[u64]) -> Result<()> {
        let batch_list = batch_list.to_vec();
        self.spawn(move |store| store.remove_chunks_batch(&batch_list))
            .await
    }

    pub async fn update_shard_config(&self, shard_config: ShardConfig) {
        self.spawn(move |store| {
            store.flow().update_shard_config(shard_config);
            Ok(())
        })
        .await
        .expect("always ok")
    }

    async fn spawn<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&dyn LogStore) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let store = self.store.clone();
        let (tx, rx) = oneshot::channel();

        self.executor.spawn_blocking(
            move || {
                // FIXME(zz): Not all functions need `write`. Refactor store usage.
                let res = f(&*store);

                if tx.send(res).is_err() {
                    error!("Unable to complete async storage operation: the receiver dropped");
                }
            },
            WORKER_TASK_NAME,
        );

        rx.await
            .unwrap_or_else(|_| bail!(error::Error::Custom("Receiver error".to_string())))
    }

    // FIXME(zz): Refactor the lock and async call here.
    pub fn get_store(&self) -> &dyn LogStore {
        self.store.as_ref()
    }
}
