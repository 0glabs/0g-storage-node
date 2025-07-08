use crate::config::ShardConfig;
use crate::log_store::flow_store::{
    batch_iter_sharded, FlowConfig, FlowDBStore, FlowStore, PadPair,
};
use crate::log_store::tx_store::{BlockHashAndSubmissionIndex, TransactionStore, TxStatus};
use crate::log_store::{
    FlowRead, FlowSeal, FlowWrite, LogStoreChunkRead, LogStoreChunkWrite, LogStoreRead,
    LogStoreWrite, MineLoadChunk, SealAnswer, SealTask,
};
use crate::{try_option, ZgsKeyValueDB};
use anyhow::{anyhow, bail, Result};
use append_merkle::{Algorithm, MerkleTreeRead, Sha3Algorithm};
use ethereum_types::H256;
use kvdb_rocksdb::{Database, DatabaseConfig};
use merkle_light::merkle::{log2_pow2, MerkleTree};
use merkle_tree::RawLeafSha3Algorithm;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use rayon::iter::ParallelIterator;
use rayon::prelude::ParallelSlice;
use shared_types::{
    bytes_to_chunks, compute_padded_chunk_size, compute_segment_size, Chunk, ChunkArray,
    ChunkArrayWithProof, ChunkWithProof, DataRoot, FlowProof, FlowRangeProof, Merkle, Transaction,
};
use std::cmp::Ordering;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, instrument, trace, warn};

use crate::log_store::metrics;

/// 256 Bytes
pub const ENTRY_SIZE: usize = 256;
/// 1024 Entries.
pub const PORA_CHUNK_SIZE: usize = 1024;

pub const COL_TX: u32 = 0; // flow db
pub const COL_ENTRY_BATCH: u32 = 1; // data db
pub const COL_TX_DATA_ROOT_INDEX: u32 = 2; // flow db
pub const COL_TX_COMPLETED: u32 = 3; // data db
pub const COL_MISC: u32 = 4; // flow db & data db
pub const COL_FLOW_MPT_NODES: u32 = 5; // flow db
pub const COL_BLOCK_PROGRESS: u32 = 6; // flow db
pub const COL_PAD_DATA_LIST: u32 = 7; // flow db
pub const COL_PAD_DATA_SYNC_HEIGH: u32 = 8; // data db
pub const COL_NUM: u32 = 9;

pub const DATA_DB_KEY: &str = "data_db";
pub const FLOW_DB_KEY: &str = "flow_db";
const PAD_DELAY: Duration = Duration::from_secs(2);

// Process at most 1M entries (256MB) pad data at a time.
const PAD_MAX_SIZE: usize = 1 << 20;

static PAD_SEGMENT_ROOT: Lazy<H256> = Lazy::new(|| {
    Merkle::new(
        data_to_merkle_leaves(&[0; ENTRY_SIZE * PORA_CHUNK_SIZE]).unwrap(),
        0,
        None,
    )
    .root()
});
pub struct UpdateFlowMessage {
    pub pad_data: usize,
    pub tx_start_flow_index: u64,
}

pub struct LogManager {
    pub(crate) flow_db: Arc<dyn ZgsKeyValueDB>,
    pub(crate) data_db: Arc<dyn ZgsKeyValueDB>,
    tx_store: TransactionStore,
    flow_store: Arc<FlowStore>,
    merkle: RwLock<MerkleManager>,
}

struct MerkleManager {
    // TODO(zz): Refactor the in-memory merkle and in-disk storage together.
    pora_chunks_merkle: Merkle,
    /// The in-memory structure of the sub merkle tree of the last chunk.
    /// The size is always less than `PORA_CHUNK_SIZE`.
    last_chunk_merkle: Merkle,
}

impl MerkleManager {
    fn last_chunk_start_index(&self) -> u64 {
        if self.pora_chunks_merkle.leaves() == 0 {
            0
        } else {
            PORA_CHUNK_SIZE as u64
                * if self.last_chunk_merkle.leaves() == 0 {
                    // The last chunk is empty and its root hash is not in `pora_chunk_merkle`,
                    // so all chunks in `pora_chunk_merkle` is complete.
                    self.pora_chunks_merkle.leaves()
                } else {
                    // The last chunk has data, so we need to exclude it from `pora_chunks_merkle`.
                    self.pora_chunks_merkle.leaves() - 1
                } as u64
        }
    }

    #[instrument(skip(self))]
    fn commit_merkle(&mut self, tx_seq: u64) -> Result<()> {
        self.pora_chunks_merkle.commit(Some(tx_seq));
        self.last_chunk_merkle.commit(Some(tx_seq));
        Ok(())
    }

    fn revert_merkle_tree(&mut self, tx_seq: u64, tx_store: &TransactionStore) -> Result<()> {
        debug!("revert merkle tree {}", tx_seq);
        // Special case for reverting tx_seq == 0
        if tx_seq == u64::MAX {
            self.pora_chunks_merkle.reset();
            self.last_chunk_merkle.reset();
            return Ok(());
        }
        let old_leaves = self.pora_chunks_merkle.leaves();
        self.pora_chunks_merkle.revert_to(tx_seq)?;
        if old_leaves == self.pora_chunks_merkle.leaves() {
            self.last_chunk_merkle.revert_to(tx_seq)?;
        } else {
            // We are reverting to a position before the current last_chunk.
            self.last_chunk_merkle =
                tx_store.rebuild_last_chunk_merkle(self.pora_chunks_merkle.leaves() - 1, tx_seq)?;
        }
        Ok(())
    }

    fn try_initialize(&mut self, flow_store: &FlowStore) -> Result<()> {
        if self.pora_chunks_merkle.leaves() == 0 && self.last_chunk_merkle.leaves() == 0 {
            self.last_chunk_merkle.append(H256::zero());
            self.pora_chunks_merkle
                .update_last(self.last_chunk_merkle.root());
        } else if self.last_chunk_merkle.leaves() != 0 {
            let last_chunk_start_index = self.last_chunk_start_index();
            let last_chunk_data = flow_store.get_available_entries(
                last_chunk_start_index,
                last_chunk_start_index + PORA_CHUNK_SIZE as u64,
            )?;
            for e in last_chunk_data {
                let start_index = e.start_index - last_chunk_start_index;
                for i in 0..e.data.len() / ENTRY_SIZE {
                    let index = i + start_index as usize;
                    if index >= self.last_chunk_merkle.leaves() {
                        // We revert the merkle tree before truncate the flow store,
                        // so last_chunk_data may include data that should have been truncated.
                        break;
                    }
                    self.last_chunk_merkle.fill_leaf(
                        index,
                        Sha3Algorithm::leaf(&e.data[i * ENTRY_SIZE..(i + 1) * ENTRY_SIZE]),
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct LogConfig {
    pub flow: FlowConfig,
}

impl LogStoreChunkWrite for LogManager {
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()> {
        let mut merkle = self.merkle.write();
        let tx = self
            .tx_store
            .get_tx_by_seq_number(tx_seq)?
            .ok_or_else(|| anyhow!("put chunks with missing tx: tx_seq={}", tx_seq))?;
        let (chunks_for_proof, _) = compute_padded_chunk_size(tx.size as usize);
        if chunks.start_index.saturating_mul(ENTRY_SIZE as u64) + chunks.data.len() as u64
            > (chunks_for_proof * ENTRY_SIZE) as u64
        {
            bail!(
                "put chunks with data out of tx range: tx_seq={} start_index={} data_len={}",
                tx_seq,
                chunks.start_index,
                chunks.data.len()
            );
        }
        // TODO: Use another struct to avoid confusion.
        let mut flow_entry_array = chunks;
        flow_entry_array.start_index += tx.start_entry_index;
        self.append_entries(flow_entry_array, &mut merkle)?;
        Ok(())
    }

    fn put_chunks_with_tx_hash(
        &self,
        tx_seq: u64,
        tx_hash: H256,
        chunks: ChunkArray,
        maybe_file_proof: Option<FlowProof>,
    ) -> Result<bool> {
        let start_time = Instant::now();
        let mut merkle = self.merkle.write();
        let tx = self
            .tx_store
            .get_tx_by_seq_number(tx_seq)?
            .ok_or_else(|| anyhow!("put chunks with missing tx: tx_seq={}", tx_seq))?;
        if tx.hash() != tx_hash {
            return Ok(false);
        }
        let (chunks_for_proof, _) = compute_padded_chunk_size(tx.size as usize);
        if chunks.start_index.saturating_mul(ENTRY_SIZE as u64) + chunks.data.len() as u64
            > (chunks_for_proof * ENTRY_SIZE) as u64
        {
            bail!(
                "put chunks with data out of tx range: tx_seq={} start_index={} data_len={}",
                tx_seq,
                chunks.start_index,
                chunks.data.len()
            );
        }
        // TODO: Use another struct to avoid confusion.
        let mut flow_entry_array = chunks;
        flow_entry_array.start_index += tx.start_entry_index;
        self.append_entries(flow_entry_array, &mut merkle)?;

        if let Some(file_proof) = maybe_file_proof {
            merkle.pora_chunks_merkle.fill_with_file_proof(
                file_proof,
                tx.merkle_nodes,
                tx.start_entry_index,
            )?;
        }
        metrics::PUT_CHUNKS.update_since(start_time);
        Ok(true)
    }

    fn remove_chunks_batch(&self, batch_list: &[u64]) -> crate::error::Result<()> {
        self.flow_store.delete_batch_list(batch_list)
    }
}

impl LogStoreWrite for LogManager {
    #[instrument(skip(self))]
    /// Insert the tx and update the flow store if needed.
    ///
    /// We assumes that all transactions are inserted in order sequentially.
    /// We always write the database in the following order:
    /// 1. Insert the tx (the tx and the root to tx_seq map are inserted atomically).
    /// 2. Update the flow store(pad data for alignment and copy data in `put_tx`, write data in
    /// `put_chunks`, pad rear data in `finalize_tx`).
    /// 3. Mark tx as finalized.
    ///
    /// Step 1 and 3 are both atomic operations.
    /// * If a tx has been finalized, the data in flow must
    /// have been updated correctly.
    /// * If `put_tx` succeeds but not finalized, we rely on the upper layer
    /// operations (client/auto-sync) to insert needed data (`put_chunks`) and trigger
    /// finalization (`finalize_tx`).
    /// * If `put_tx` fails in the middle, the tx is inserted but the flow is not updated correctly.
    /// Only the last tx may have this case, so we rerun
    /// `put_tx` for the last tx when we restart the node to ensure that it succeeds.
    ///
    fn put_tx(&self, tx: Transaction) -> Result<()> {
        let start_time = Instant::now();
        let mut merkle = self.merkle.write();
        debug!("put_tx: tx={:?}", tx);
        let expected_seq = self.tx_store.next_tx_seq();
        if tx.seq != expected_seq {
            if tx.seq + 1 == expected_seq && !self.check_tx_completed(tx.seq)? {
                // special case for rerun the last tx during recovery.
                debug!("recovery with tx_seq={}", tx.seq);
            } else {
                // This is not supposed to happen since we have checked the tx seq in log entry sync.
                error!("tx mismatch, expected={} get={:?}", expected_seq, tx);
                bail!("unexpected tx!");
            }
        }
        let maybe_same_data_tx_seq = self.tx_store.put_tx(tx.clone())?.first().cloned();
        // TODO(zz): Should we validate received tx?
        self.append_subtree_list(
            tx.seq,
            tx.start_entry_index,
            tx.merkle_nodes.clone(),
            &mut merkle,
        )?;
        merkle.commit_merkle(tx.seq)?;
        debug!(
            "commit flow root: root={:?}",
            merkle.pora_chunks_merkle.root()
        );
        // Drop the lock because `copy_tx_data` will lock again.
        drop(merkle);

        if let Some(old_tx_seq) = maybe_same_data_tx_seq {
            if self.check_tx_completed(old_tx_seq)? {
                // copy and finalize once, then stop
                self.copy_tx_and_finalize(old_tx_seq, vec![tx.seq])?;
            }
        }
        metrics::PUT_TX.update_since(start_time);
        Ok(())
    }

    fn finalize_tx(&self, tx_seq: u64) -> Result<()> {
        let tx = self
            .tx_store
            .get_tx_by_seq_number(tx_seq)?
            .ok_or_else(|| anyhow!("finalize_tx with tx missing: tx_seq={}", tx_seq))?;

        self.padding_rear_data(&tx)?;

        let tx_end_index = tx.start_entry_index + bytes_to_entries(tx.size);
        // TODO: Check completeness without loading all data in memory.
        // TODO: Should we double check the tx merkle root?
        if self.check_data_completed(tx.start_entry_index, tx_end_index)? {
            let same_root_seq_list = self
                .tx_store
                .get_tx_seq_list_by_data_root(&tx.data_merkle_root)?;
            // Check if there are other same-root transaction not finalized.
            if same_root_seq_list.first() == Some(&tx_seq) {
                // If this is the first tx with this data root, copy and finalize all same-root txs.
                self.copy_tx_and_finalize(tx_seq, same_root_seq_list[1..].to_vec())?;
            } else {
                // If this is not the first tx with this data root, and the first one is not finalized.
                let maybe_first_seq = same_root_seq_list.first().cloned();
                if let Some(first_seq) = maybe_first_seq {
                    if !self.check_tx_completed(first_seq)? {
                        self.copy_tx_and_finalize(tx_seq, same_root_seq_list)?;
                    }
                }
            }

            self.tx_store.finalize_tx(tx_seq)?;
            Ok(())
        } else {
            bail!("finalize tx with data missing: tx_seq={}", tx_seq)
        }
    }

    fn finalize_tx_with_hash(&self, tx_seq: u64, tx_hash: H256) -> crate::error::Result<bool> {
        let start_time = Instant::now();
        trace!(
            "finalize_tx_with_hash: tx_seq={} tx_hash={:?}",
            tx_seq,
            tx_hash
        );
        let tx = self
            .tx_store
            .get_tx_by_seq_number(tx_seq)?
            .ok_or_else(|| anyhow!("finalize_tx with tx missing: tx_seq={}", tx_seq))?;
        debug!("finalize_tx_with_hash: tx={:?}", tx);
        if tx.hash() != tx_hash {
            return Ok(false);
        }

        self.padding_rear_data(&tx)?;

        // TODO: Check completeness without loading all data in memory.
        // TODO: Should we double check the tx merkle root?
        let tx_end_index = tx.start_entry_index + bytes_to_entries(tx.size);
        if self.check_data_completed(tx.start_entry_index, tx_end_index)? {
            let same_root_seq_list = self
                .tx_store
                .get_tx_seq_list_by_data_root(&tx.data_merkle_root)?;
            // Check if there are other same-root transaction not finalized.

            if same_root_seq_list.first() == Some(&tx_seq) {
                self.copy_tx_and_finalize(tx_seq, same_root_seq_list[1..].to_vec())?;
            } else {
                // If this is not the first tx with this data root, copy and finalize the first one.
                let maybe_first_seq = same_root_seq_list.first().cloned();
                if let Some(first_seq) = maybe_first_seq {
                    if !self.check_tx_completed(first_seq)? {
                        self.copy_tx_and_finalize(tx_seq, same_root_seq_list)?;
                    }
                }
            }

            self.tx_store.finalize_tx(tx_seq)?;

            metrics::FINALIZE_TX_WITH_HASH.update_since(start_time);
            Ok(true)
        } else {
            bail!("finalize tx hash with data missing: tx_seq={}", tx_seq)
        }
    }

    fn prune_tx(&self, tx_seq: u64) -> crate::error::Result<()> {
        self.tx_store.prune_tx(tx_seq)
    }

    fn put_sync_progress(&self, progress: (u64, H256, Option<Option<u64>>)) -> Result<()> {
        self.tx_store.put_progress(progress)
    }

    fn put_log_latest_block_number(&self, block_number: u64) -> Result<()> {
        self.tx_store.put_log_latest_block_number(block_number)
    }

    /// Return the reverted Transactions in order.
    /// `tx_seq == u64::MAX` is a special case for reverting all transactions.
    fn revert_to(&self, tx_seq: u64) -> Result<Vec<Transaction>> {
        // FIXME(zz): If this revert is triggered by chain reorg after restarts, this will fail.
        let mut merkle = self.merkle.write();
        merkle.revert_merkle_tree(tx_seq, &self.tx_store)?;
        merkle.try_initialize(&self.flow_store)?;
        assert_eq!(
            Some(merkle.last_chunk_merkle.root()),
            merkle
                .pora_chunks_merkle
                .leaf_at(merkle.pora_chunks_merkle.leaves() - 1)?
        );
        let start_index = merkle.last_chunk_start_index() * PORA_CHUNK_SIZE as u64
            + merkle.last_chunk_merkle.leaves() as u64;
        self.flow_store.truncate(start_index)?;
        let start = if tx_seq != u64::MAX { tx_seq + 1 } else { 0 };
        self.tx_store.remove_tx_after(start)
    }

    fn validate_and_insert_range_proof(
        &self,
        tx_seq: u64,
        data: &ChunkArrayWithProof,
    ) -> Result<bool> {
        let valid = self.validate_range_proof(tx_seq, data)?;
        // `merkle` is used in `validate_range_proof`.
        let mut merkle = self.merkle.write();
        if valid {
            merkle
                .pora_chunks_merkle
                .fill_with_range_proof(data.proof.clone())?;
        }
        Ok(valid)
    }

    fn delete_block_hash_by_number(&self, block_number: u64) -> Result<()> {
        self.tx_store.delete_block_hash_by_number(block_number)
    }

    fn update_shard_config(&self, shard_config: ShardConfig) {
        self.flow_store.update_shard_config(shard_config)
    }

    fn submit_seal_result(&self, answers: Vec<SealAnswer>) -> Result<()> {
        self.flow_store.submit_seal_result(answers)
    }

    fn start_padding(&self, executor: &task_executor::TaskExecutor) {
        let store = self.flow_store.clone();
        executor.spawn(
            async move {
                let current_height = store.get_pad_data_sync_height().unwrap();
                let mut start_index = current_height.unwrap_or(0);
                loop {
                    match store.get_pad_data(start_index) {
                        std::result::Result::Ok(data) => {
                            // Update the flow database.
                            // This should be called before `complete_last_chunk_merkle` so that we do not save
                            // subtrees with data known.
                            if let Some(data) = data {
                                for pad in data {
                                    store
                                        .append_entries(ChunkArray {
                                            data: vec![0; pad.data_size as usize],
                                            start_index: pad.start_index,
                                        })
                                        .unwrap();
                                }
                            };
                            store.put_pad_data_sync_height(start_index).unwrap();
                            start_index += 1;
                        }
                        std::result::Result::Err(_) => {
                            debug!("Unable to get pad data, start_index={}", start_index);
                            tokio::time::sleep(PAD_DELAY).await;
                        }
                    };
                }
            },
            "pad_tx",
        );
    }
}

impl LogStoreChunkRead for LogManager {
    fn get_chunk_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> crate::error::Result<Option<Chunk>> {
        // TODO(zz): This is not needed?
        let single_chunk_array =
            try_option!(self.get_chunks_by_tx_and_index_range(tx_seq, index, index + 1)?);
        Ok(Some(Chunk(single_chunk_array.data.as_slice().try_into()?)))
    }

    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> crate::error::Result<Option<ChunkArray>> {
        let tx = try_option!(self.get_tx_by_seq_number(tx_seq)?);

        if index_end as u64 > bytes_to_entries(tx.size) {
            bail!(
                "end entry index exceeds tx size: end={} tx size={}",
                index_start,
                tx.size
            );
        }

        let start_flow_index = tx.start_entry_index + index_start as u64;
        let end_flow_index = tx.start_entry_index + index_end as u64;
        // TODO: Use another struct.
        // Set returned chunk start index as the offset in the tx data.
        let mut tx_chunk = try_option!(self
            .flow_store
            .get_entries(start_flow_index, end_flow_index)?);
        tx_chunk.start_index -= tx.start_entry_index;
        Ok(Some(tx_chunk))
    }

    fn get_chunk_by_data_root_and_index(
        &self,
        _data_root: &DataRoot,
        _index: usize,
    ) -> crate::error::Result<Option<Chunk>> {
        todo!()
    }

    fn get_chunks_by_data_root_and_index_range(
        &self,
        data_root: &DataRoot,
        index_start: usize,
        index_end: usize,
    ) -> crate::error::Result<Option<ChunkArray>> {
        let tx_seq = try_option!(self.get_tx_seq_by_data_root(data_root, true)?);
        self.get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)
    }

    fn get_chunk_index_list(&self, _tx_seq: u64) -> crate::error::Result<Vec<usize>> {
        todo!()
    }

    fn get_chunk_by_flow_index(
        &self,
        index: u64,
        length: u64,
    ) -> crate::error::Result<Option<ChunkArray>> {
        let start_flow_index = index;
        let end_flow_index = index + length;
        self.flow_store
            .get_entries(start_flow_index, end_flow_index)
    }
}

impl LogStoreRead for LogManager {
    fn get_tx_by_seq_number(&self, seq: u64) -> crate::error::Result<Option<Transaction>> {
        self.tx_store.get_tx_by_seq_number(seq)
    }

    fn get_tx_seq_by_data_root(
        &self,
        data_root: &DataRoot,
        need_available: bool,
    ) -> crate::error::Result<Option<u64>> {
        let seq_list = self.tx_store.get_tx_seq_list_by_data_root(data_root)?;
        let mut available_seq = None;
        for tx_seq in &seq_list {
            if self.tx_store.check_tx_completed(*tx_seq)? {
                // Return the first finalized tx if possible.
                return Ok(Some(*tx_seq));
            }
            if need_available
                && available_seq.is_none()
                && !self.tx_store.check_tx_pruned(*tx_seq)?
            {
                available_seq = Some(*tx_seq);
            }
        }
        if need_available {
            return Ok(available_seq);
        }
        // No tx is finalized, return the first one.
        Ok(seq_list.first().cloned())
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> crate::error::Result<Option<ChunkWithProof>> {
        // TODO(zz): Optimize for mining.
        let single_chunk_array = try_option!(self.get_chunks_with_proof_by_tx_and_index_range(
            tx_seq,
            index,
            index + 1,
            None
        )?);
        Ok(Some(ChunkWithProof {
            chunk: Chunk(single_chunk_array.chunks.data.as_slice().try_into()?),
            proof: single_chunk_array.proof.left_proof,
        }))
    }

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
        merkle_tx_seq: Option<u64>,
    ) -> crate::error::Result<Option<ChunkArrayWithProof>> {
        let tx = try_option!(self.tx_store.get_tx_by_seq_number(tx_seq)?);
        let chunks =
            try_option!(self.get_chunks_by_tx_and_index_range(tx_seq, index_start, index_end)?);
        let left_proof =
            self.gen_proof_at_version(tx.start_entry_index + index_start as u64, merkle_tx_seq)?;
        let right_proof =
            self.gen_proof_at_version(tx.start_entry_index + index_end as u64 - 1, merkle_tx_seq)?;
        Ok(Some(ChunkArrayWithProof {
            chunks,
            proof: FlowRangeProof {
                left_proof,
                right_proof,
            },
        }))
    }

    fn get_tx_status(&self, tx_seq: u64) -> Result<Option<TxStatus>> {
        self.tx_store.get_tx_status(tx_seq)
    }

    fn check_tx_completed(&self, tx_seq: u64) -> crate::error::Result<bool> {
        self.tx_store.check_tx_completed(tx_seq)
    }

    fn validate_range_proof(&self, tx_seq: u64, data: &ChunkArrayWithProof) -> Result<bool> {
        let tx = self
            .get_tx_by_seq_number(tx_seq)?
            .ok_or_else(|| anyhow!("tx missing"))?;
        let leaves = data_to_merkle_leaves(&data.chunks.data)?;
        data.proof.validate::<Sha3Algorithm>(
            &leaves,
            (data.chunks.start_index + tx.start_entry_index) as usize,
        )?;
        Ok(self
            .merkle
            .read_recursive()
            .pora_chunks_merkle
            .check_root(&data.proof.root()))
    }

    fn get_sync_progress(&self) -> Result<Option<(u64, H256)>> {
        self.tx_store.get_progress()
    }

    fn get_log_latest_block_number(&self) -> Result<Option<u64>> {
        self.tx_store.get_log_latest_block_number()
    }

    fn get_block_hash_by_number(&self, block_number: u64) -> Result<Option<(H256, Option<u64>)>> {
        self.tx_store.get_block_hash_by_number(block_number)
    }

    fn get_block_hashes(&self) -> Result<Vec<(u64, BlockHashAndSubmissionIndex)>> {
        self.tx_store.get_block_hashes()
    }

    fn next_tx_seq(&self) -> u64 {
        self.tx_store.next_tx_seq()
    }

    fn get_proof_at_root(
        &self,
        root: Option<DataRoot>,
        index: u64,
        length: u64,
    ) -> crate::error::Result<FlowRangeProof> {
        let left_proof = self.gen_proof(index, root)?;
        let right_proof = self.gen_proof(index + length - 1, root)?;
        Ok(FlowRangeProof {
            left_proof,
            right_proof,
        })
    }

    fn get_context(&self) -> crate::error::Result<(DataRoot, u64)> {
        let merkle = self.merkle.read_recursive();
        Ok((
            merkle.pora_chunks_merkle.root(),
            merkle.last_chunk_start_index() + merkle.last_chunk_merkle.leaves() as u64,
        ))
    }

    fn check_tx_pruned(&self, tx_seq: u64) -> crate::error::Result<bool> {
        self.tx_store.check_tx_pruned(tx_seq)
    }

    fn pull_seal_chunk(&self, seal_index_max: usize) -> Result<Option<Vec<SealTask>>> {
        self.flow_store.pull_seal_chunk(seal_index_max)
    }

    fn get_num_entries(&self) -> Result<u64> {
        self.flow_store.get_num_entries()
    }

    fn load_sealed_data(&self, chunk_index: u64) -> Result<Option<MineLoadChunk>> {
        self.flow_store.load_sealed_data(chunk_index)
    }

    fn get_shard_config(&self) -> ShardConfig {
        self.flow_store.get_shard_config()
    }
}

impl LogManager {
    pub fn rocksdb(
        config: LogConfig,
        flow_path: impl AsRef<Path>,
        data_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let mut db_config = DatabaseConfig::with_columns(COL_NUM);
        db_config.enable_statistics = true;
        let flow_db_source = Arc::new(Database::open(&db_config, flow_path)?);
        let data_db_source = Arc::new(Database::open(&db_config, data_path)?);
        Self::new(flow_db_source, data_db_source, config)
    }

    pub fn memorydb(config: LogConfig) -> Result<Self> {
        let flow_db = Arc::new(kvdb_memorydb::create(COL_NUM));
        let data_db = Arc::new(kvdb_memorydb::create(COL_NUM));
        Self::new(flow_db, data_db, config)
    }

    fn new(
        flow_db_source: Arc<dyn ZgsKeyValueDB>,
        data_db_source: Arc<dyn ZgsKeyValueDB>,
        config: LogConfig,
    ) -> Result<Self> {
        let tx_store = TransactionStore::new(flow_db_source.clone(), data_db_source.clone())?;
        let flow_db = Arc::new(FlowDBStore::new(flow_db_source.clone()));
        let data_db = Arc::new(FlowDBStore::new(data_db_source.clone()));
        let flow_store = Arc::new(FlowStore::new(
            flow_db.clone(),
            data_db.clone(),
            config.flow.clone(),
        ));
        // If the last tx `put_tx` does not complete, we will revert it in `pora_chunks_merkle`
        // first and call `put_tx` later.
        let next_tx_seq = tx_store.next_tx_seq();
        let mut start_tx_seq = if next_tx_seq > 0 {
            Some(next_tx_seq - 1)
        } else {
            None
        };
        let mut last_tx_to_insert = None;

        let mut pora_chunks_merkle = Merkle::new_with_subtrees(
            flow_db,
            config.flow.merkle_node_cache_capacity,
            log2_pow2(PORA_CHUNK_SIZE),
        )?;
        if let Some(last_tx_seq) = start_tx_seq {
            if !tx_store.check_tx_completed(last_tx_seq)? {
                // Last tx not finalized, we need to check if its `put_tx` is completed.
                let last_tx = tx_store
                    .get_tx_by_seq_number(last_tx_seq)?
                    .expect("tx missing");
                let current_len = pora_chunks_merkle.leaves();
                let expected_len = sector_to_segment(
                    last_tx.start_entry_index
                        + last_tx.num_entries() as u64
                        + PORA_CHUNK_SIZE as u64
                        - 1,
                );
                match expected_len.cmp(&(current_len)) {
                    Ordering::Less => {
                        bail!(
                            "Unexpected DB: merkle tree larger than the known data size,\
                        expected={} get={}",
                            expected_len,
                            current_len
                        );
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        // Flow updates are not complete.
                        // For simplicity, we build the merkle tree for the previous tx and update
                        // the flow for the last tx again.
                        info!("revert last tx: last_tx={:?}", last_tx);
                        last_tx_to_insert = Some(last_tx);
                        if last_tx_seq == 0 {
                            start_tx_seq = None;
                        } else {
                            // truncate until we get the pora chunks merkle for the previous tx.
                            let previous_tx = tx_store
                                .get_tx_by_seq_number(last_tx_seq - 1)?
                                .expect("tx missing");
                            let expected_len = sector_to_segment(
                                previous_tx.start_entry_index + previous_tx.num_entries() as u64,
                            );
                            if current_len > expected_len {
                                pora_chunks_merkle.revert_to_leaves(expected_len)?;
                            } else {
                                assert_eq!(current_len, expected_len);
                            }
                            start_tx_seq = Some(previous_tx.seq);
                        };
                    }
                }
            }
        }

        let last_chunk_merkle = match start_tx_seq {
            Some(tx_seq) => {
                let tx = tx_store.get_tx_by_seq_number(tx_seq)?.expect("tx missing");
                if (tx.start_entry_index() + tx.num_entries() as u64) % PORA_CHUNK_SIZE as u64 == 0
                {
                    // The last chunk should be aligned, so it's empty.
                    Merkle::new_with_depth(vec![], log2_pow2(PORA_CHUNK_SIZE) + 1, None)
                } else {
                    tx_store.rebuild_last_chunk_merkle(pora_chunks_merkle.leaves() - 1, tx_seq)?
                }
            }
            // Initialize
            None => {
                pora_chunks_merkle.reset();
                Merkle::new_with_depth(vec![], 1, None)
            }
        };

        debug!(
            "LogManager::new() with chunk_list_len={} start_tx_seq={:?} last_chunk={}",
            pora_chunks_merkle.leaves(),
            start_tx_seq,
            last_chunk_merkle.leaves(),
        );
        if last_chunk_merkle.leaves() != 0 {
            pora_chunks_merkle.update_last(last_chunk_merkle.root());
        }
        // update the merkle root
        pora_chunks_merkle.commit(start_tx_seq);
        let merkle = RwLock::new(MerkleManager {
            pora_chunks_merkle,
            last_chunk_merkle,
        });

        let log_manager = Self {
            flow_db: flow_db_source,
            data_db: data_db_source,
            tx_store,
            flow_store,
            merkle,
        };

        if let Some(tx) = last_tx_to_insert {
            log_manager.put_tx(tx)?;
        }
        log_manager
            .merkle
            .write()
            .try_initialize(&log_manager.flow_store)?;
        info!(
            "Log manager initialized, state={:?}",
            log_manager.get_context()?
        );
        Ok(log_manager)
    }

    fn gen_proof(&self, flow_index: u64, maybe_root: Option<DataRoot>) -> Result<FlowProof> {
        match maybe_root {
            None => self.gen_proof_at_version(flow_index, None),
            Some(root) => {
                let merkle = self.merkle.read_recursive();
                let tx_seq = merkle.pora_chunks_merkle.tx_seq_at_root(&root)?;
                self.gen_proof_at_version(flow_index, Some(tx_seq))
            }
        }
    }

    fn gen_proof_at_version(
        &self,
        flow_index: u64,
        maybe_tx_seq: Option<u64>,
    ) -> Result<FlowProof> {
        let merkle = self.merkle.read_recursive();
        let seg_index = sector_to_segment(flow_index);
        let top_proof = match maybe_tx_seq {
            None => merkle.pora_chunks_merkle.gen_proof(seg_index)?,
            Some(tx_seq) => merkle
                .pora_chunks_merkle
                .at_version(tx_seq)?
                .gen_proof(seg_index)?,
        };

        // TODO(zz): Maybe we can decide that all proofs are at the PoRA chunk level, so
        // we do not need to maintain the proof at the entry level below.
        // Condition (self.last_chunk_merkle.leaves() == 0): When last chunk size is exactly PORA_CHUNK_SIZE, proof should be generated from flow data, as last_chunk_merkle.leaves() is zero at this time
        // TODO(zz): In the current use cases, `maybe_root` is only `Some` for mining
        // and `flow_index` must be within a complete PoRA chunk. For possible future usages,
        // we'll need to find the flow length at the given root and load a partial chunk
        // if `flow_index` is in the last chunk.
        let sub_proof = if seg_index != merkle.pora_chunks_merkle.leaves() - 1
            || merkle.last_chunk_merkle.leaves() == 0
        {
            self.flow_store
                .gen_proof_in_batch(seg_index, flow_index as usize % PORA_CHUNK_SIZE)?
        } else {
            match maybe_tx_seq {
                None => merkle
                    .last_chunk_merkle
                    .gen_proof(flow_index as usize % PORA_CHUNK_SIZE)?,
                Some(tx_version) => merkle
                    .last_chunk_merkle
                    .at_version(tx_version)?
                    .gen_proof(flow_index as usize % PORA_CHUNK_SIZE)?,
            }
        };
        entry_proof(&top_proof, &sub_proof)
    }

    #[instrument(skip(self, merkle))]
    fn append_subtree_list(
        &self,
        tx_seq: u64,
        tx_start_index: u64,
        merkle_list: Vec<(usize, DataRoot)>,
        merkle: &mut MerkleManager,
    ) -> Result<()> {
        if merkle_list.is_empty() {
            return Ok(());
        }
        let start_time = Instant::now();

        self.pad_tx(tx_seq, tx_start_index, &mut *merkle)?;

        for (subtree_depth, subtree_root) in merkle_list {
            let subtree_size = 1 << (subtree_depth - 1);
            if merkle.last_chunk_merkle.leaves() + subtree_size <= PORA_CHUNK_SIZE {
                merkle
                    .last_chunk_merkle
                    .append_subtree(subtree_depth, subtree_root)?;
                if merkle.last_chunk_merkle.leaves() == subtree_size {
                    // `last_chunk_merkle` was empty, so this is a new leaf in the top_tree.
                    merkle
                        .pora_chunks_merkle
                        .append_subtree(1, merkle.last_chunk_merkle.root())?;
                } else {
                    merkle
                        .pora_chunks_merkle
                        .update_last(merkle.last_chunk_merkle.root());
                }
                if merkle.last_chunk_merkle.leaves() == PORA_CHUNK_SIZE {
                    self.complete_last_chunk_merkle(
                        merkle.pora_chunks_merkle.leaves() - 1,
                        &mut *merkle,
                    )?;
                }
            } else {
                // `last_chunk_merkle` has been padded here, so a subtree should not be across
                // the chunks boundary.
                assert_eq!(merkle.last_chunk_merkle.leaves(), 0);
                assert!(subtree_size >= PORA_CHUNK_SIZE);
                merkle
                    .pora_chunks_merkle
                    .append_subtree(subtree_depth - log2_pow2(PORA_CHUNK_SIZE), subtree_root)?;
            }
        }

        metrics::APPEND_SUBTREE_LIST.update_since(start_time);
        Ok(())
    }

    #[instrument(skip(self, merkle))]
    fn pad_tx(&self, tx_seq: u64, tx_start_index: u64, merkle: &mut MerkleManager) -> Result<()> {
        // Check if we need to pad the flow.
        let start_time = Instant::now();
        let mut tx_start_flow_index =
            merkle.last_chunk_start_index() + merkle.last_chunk_merkle.leaves() as u64;
        let pad_size = tx_start_index - tx_start_flow_index;
        trace!(
            "before pad_tx {} {}",
            merkle.pora_chunks_merkle.leaves(),
            merkle.last_chunk_merkle.leaves()
        );
        let mut pad_list = vec![];
        if pad_size != 0 {
            for pad_data in Self::padding(pad_size as usize) {
                let mut is_full_empty = true;

                // Update the in-memory merkle tree.
                let last_chunk_pad = if merkle.last_chunk_merkle.leaves() == 0 {
                    0
                } else {
                    (PORA_CHUNK_SIZE - merkle.last_chunk_merkle.leaves()) * ENTRY_SIZE
                };

                let mut completed_chunk_index = None;
                if pad_data.len() < last_chunk_pad {
                    is_full_empty = false;
                    merkle
                        .last_chunk_merkle
                        .append_list(data_to_merkle_leaves(&pad_data)?);
                    merkle
                        .pora_chunks_merkle
                        .update_last(merkle.last_chunk_merkle.root());
                } else {
                    if last_chunk_pad != 0 {
                        is_full_empty = false;
                        // Pad the last chunk.
                        merkle
                            .last_chunk_merkle
                            .append_list(data_to_merkle_leaves(&pad_data[..last_chunk_pad])?);
                        merkle
                            .pora_chunks_merkle
                            .update_last(merkle.last_chunk_merkle.root());
                        completed_chunk_index = Some(merkle.pora_chunks_merkle.leaves() - 1);
                    }

                    // Pad with more complete chunks.
                    let mut start_index = last_chunk_pad / ENTRY_SIZE;
                    while pad_data.len() >= (start_index + PORA_CHUNK_SIZE) * ENTRY_SIZE {
                        merkle.pora_chunks_merkle.append(*PAD_SEGMENT_ROOT);
                        start_index += PORA_CHUNK_SIZE;
                    }
                    assert_eq!(pad_data.len(), start_index * ENTRY_SIZE);
                }

                let data_size = pad_data.len() / ENTRY_SIZE;
                if is_full_empty {
                    pad_list.push(PadPair {
                        data_size: pad_data.len() as u64,
                        start_index: tx_start_flow_index,
                    });
                } else {
                    // Update the flow database.
                    // This should be called before `complete_last_chunk_merkle` so that we do not save
                    // subtrees with data known.
                    self.flow_store.append_entries(ChunkArray {
                        data: pad_data.to_vec(),
                        start_index: tx_start_flow_index,
                    })?;
                }

                tx_start_flow_index += data_size as u64;
                if let Some(index) = completed_chunk_index {
                    self.complete_last_chunk_merkle(index, &mut *merkle)?;
                }
            }
        }
        trace!(
            "after pad_tx {} {}",
            merkle.pora_chunks_merkle.leaves(),
            merkle.last_chunk_merkle.leaves()
        );

        self.flow_store.put_pad_data(&pad_list, tx_seq)?;

        metrics::PAD_TX.update_since(start_time);
        Ok(())
    }

    fn append_entries(
        &self,
        flow_entry_array: ChunkArray,
        merkle: &mut MerkleManager,
    ) -> Result<()> {
        let last_chunk_start_index = merkle.last_chunk_start_index();
        if flow_entry_array.start_index + bytes_to_chunks(flow_entry_array.data.len()) as u64
            > last_chunk_start_index
        {
            // Update `last_chunk_merkle` with real data.
            let (chunk_start_index, flow_entry_data_index) = if flow_entry_array.start_index
                >= last_chunk_start_index
            {
                // flow_entry_array only fill last chunk
                (
                    (flow_entry_array.start_index - last_chunk_start_index) as usize,
                    0,
                )
            } else {
                // flow_entry_array fill both last and last - 1 chunk
                (
                    0,
                    (last_chunk_start_index - flow_entry_array.start_index) as usize * ENTRY_SIZE,
                )
            };

            // Since we always put tx before insert its data. Here `last_chunk_merkle` must
            // have included the data range.
            for (local_index, entry) in flow_entry_array.data[flow_entry_data_index..]
                .chunks_exact(ENTRY_SIZE)
                .enumerate()
            {
                merkle
                    .last_chunk_merkle
                    .fill_leaf(chunk_start_index + local_index, Sha3Algorithm::leaf(entry));
            }
            merkle
                .pora_chunks_merkle
                .update_last(merkle.last_chunk_merkle.root());
        }
        let chunk_roots = self.flow_store.append_entries(flow_entry_array)?;
        for (chunk_index, chunk_root) in chunk_roots {
            if chunk_index < merkle.pora_chunks_merkle.leaves() as u64 {
                merkle
                    .pora_chunks_merkle
                    .fill_leaf(chunk_index as usize, chunk_root);
            } else {
                // TODO(zz): This assumption may be false in the future.
                unreachable!("We always insert tx nodes before put_chunks");
            }
        }
        Ok(())
    }

    // FIXME(zz): Implement padding.
    pub fn padding(len: usize) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let remainder = len % PAD_MAX_SIZE;
        let n = len / PAD_MAX_SIZE;
        let iter = (0..n).map(|_| Self::padding_raw(PAD_MAX_SIZE));
        if remainder == 0 {
            Box::new(iter)
        } else {
            // insert the remainder to the front, so the rest are processed with alignment.
            let new_iter = vec![Self::padding_raw(remainder)].into_iter().chain(iter);
            Box::new(new_iter)
        }
    }

    pub fn padding_raw(len: usize) -> Vec<u8> {
        vec![0; len * ENTRY_SIZE]
    }

    #[cfg(test)]
    pub fn flow_store(&self) -> &FlowStore {
        &self.flow_store
    }

    fn padding_rear_data(&self, tx: &Transaction) -> Result<()> {
        let (chunks, _) = compute_padded_chunk_size(tx.size as usize);
        let (segments_for_proof, last_segment_size_for_proof) =
            compute_segment_size(chunks, PORA_CHUNK_SIZE);
        debug!(
            "segments_for_proof: {}, last_segment_size_for_proof: {}",
            segments_for_proof, last_segment_size_for_proof
        );

        let chunks_for_file = bytes_to_entries(tx.size) as usize;
        let (mut segments_for_file, mut last_segment_size_for_file) =
            compute_segment_size(chunks_for_file, PORA_CHUNK_SIZE);
        debug!(
            "segments_for_file: {}, last_segment_size_for_file: {}",
            segments_for_file, last_segment_size_for_file
        );

        while segments_for_file <= segments_for_proof {
            let padding_size = if segments_for_file == segments_for_proof {
                (last_segment_size_for_proof - last_segment_size_for_file) * ENTRY_SIZE
            } else {
                (PORA_CHUNK_SIZE - last_segment_size_for_file) * ENTRY_SIZE
            };

            debug!("Padding size: {}", padding_size);
            if padding_size > 0 {
                // This tx hash is guaranteed to be consistent.
                self.put_chunks_with_tx_hash(
                    tx.seq,
                    tx.hash(),
                    ChunkArray {
                        data: vec![0u8; padding_size],
                        start_index: ((segments_for_file - 1) * PORA_CHUNK_SIZE
                            + last_segment_size_for_file)
                            as u64,
                    },
                    None,
                )?;
            }

            last_segment_size_for_file = 0;
            segments_for_file += 1;
        }

        Ok(())
    }

    fn copy_tx_and_finalize(&self, from_tx_seq: u64, to_tx_seq_list: Vec<u64>) -> Result<()> {
        let start_time = Instant::now();

        let mut merkle = self.merkle.write();
        let shard_config = self.flow_store.get_shard_config();
        // We have all the data need for this tx, so just copy them.
        let old_tx = self
            .get_tx_by_seq_number(from_tx_seq)?
            .ok_or_else(|| anyhow!("from tx missing"))?;
        let mut to_tx_offset_list = Vec::with_capacity(to_tx_seq_list.len());

        for seq in to_tx_seq_list {
            // No need to copy data for completed tx and itself
            if self.check_tx_completed(seq)? || from_tx_seq == seq {
                continue;
            }
            let tx = self
                .get_tx_by_seq_number(seq)?
                .ok_or_else(|| anyhow!("to tx missing"))?;
            // Data for `tx` is not available due to sharding.
            if sector_to_segment(tx.start_entry_index) % shard_config.num_shard
                != sector_to_segment(old_tx.start_entry_index) % shard_config.num_shard
            {
                continue;
            }
            to_tx_offset_list.push((tx.seq, tx.start_entry_index - old_tx.start_entry_index));
        }
        if to_tx_offset_list.is_empty() {
            return Ok(());
        }
        // copy data in batches
        // TODO(zz): Do this asynchronously and keep atomicity.
        for (batch_start, batch_end) in batch_iter_sharded(
            old_tx.start_entry_index,
            old_tx.start_entry_index + old_tx.num_entries() as u64,
            PORA_CHUNK_SIZE,
            shard_config,
        ) {
            let batch_data = self
                .get_chunk_by_flow_index(batch_start, batch_end - batch_start)?
                .ok_or_else(|| anyhow!("tx data missing"))?;
            for (_, offset) in &to_tx_offset_list {
                let mut data = batch_data.clone();
                data.start_index += offset;
                self.append_entries(data, &mut merkle)?;
            }
        }
        // num_entries() includes the rear padding data, so no need for more padding.

        for (seq, _) in to_tx_offset_list {
            self.tx_store.finalize_tx(seq)?;
        }

        metrics::COPY_TX_AND_FINALIZE.update_since(start_time);
        Ok(())
    }

    /// Here we persist the subtrees with the incomplete data of the last chunk merkle so that
    /// we can still provide proof for known data in it.
    /// Another choice is to insert these subtrees earlier in `put_tx`. To insert them here can
    /// batch them and avoid inserting for the subtrees with all data known.
    fn complete_last_chunk_merkle(&self, index: usize, merkle: &mut MerkleManager) -> Result<()> {
        let subtree_list = merkle.last_chunk_merkle.get_subtrees();
        merkle.last_chunk_merkle =
            Merkle::new_with_depth(vec![], log2_pow2(PORA_CHUNK_SIZE) + 1, None);

        // Only insert non-leave subtrees. The leave data should have been available.
        let mut to_insert_subtrees = Vec::new();
        let mut start_index = 0;
        for (subtree_height, root) in subtree_list {
            to_insert_subtrees.push((start_index, subtree_height, root));
            start_index += 1 << (subtree_height - 1);
        }
        self.flow_store
            .insert_subtree_list_for_batch(index, to_insert_subtrees)
    }

    fn check_data_completed(&self, start: u64, end: u64) -> Result<bool> {
        for (batch_start, batch_end) in batch_iter_sharded(
            start,
            end,
            PORA_CHUNK_SIZE,
            self.flow_store.get_shard_config(),
        ) {
            if self
                .flow_store
                .get_entries(batch_start, batch_end)?
                .is_none()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// This represents the subtree of a chunk or the whole data merkle tree.
pub type FileMerkleTree = MerkleTree<[u8; 32], RawLeafSha3Algorithm>;

#[macro_export]
macro_rules! try_option {
    ($r: ident) => {
        match $r {
            Some(v) => v,
            None => return Ok(None),
        }
    };
    ($e: expr) => {
        match $e {
            Some(v) => v,
            None => return Ok(None),
        }
    };
}

/// This should be called with input checked.
pub fn sub_merkle_tree(leaf_data: &[u8]) -> Result<FileMerkleTree> {
    Ok(FileMerkleTree::new(
        data_to_merkle_leaves(leaf_data)?
            .into_iter()
            .map(|h| h.0)
            .collect::<Vec<[u8; 32]>>(),
    ))
}

pub fn data_to_merkle_leaves(leaf_data: &[u8]) -> Result<Vec<H256>> {
    let start_time = Instant::now();
    if leaf_data.len() % ENTRY_SIZE != 0 {
        bail!("merkle_tree: mismatched data size");
    }
    // If the data size is small, using `rayon` would introduce more overhead.
    let r = if leaf_data.len() >= ENTRY_SIZE * 8 {
        leaf_data
            .par_chunks_exact(ENTRY_SIZE)
            .map(Sha3Algorithm::leaf)
            .collect()
    } else {
        leaf_data
            .chunks_exact(ENTRY_SIZE)
            .map(Sha3Algorithm::leaf)
            .collect()
    };

    metrics::DATA_TO_MERKLE_LEAVES_SIZE.update(leaf_data.len());
    metrics::DATA_TO_MERKLE_LEAVES.update_since(start_time);
    Ok(r)
}

pub fn bytes_to_entries(size_bytes: u64) -> u64 {
    if size_bytes % ENTRY_SIZE as u64 == 0 {
        size_bytes / ENTRY_SIZE as u64
    } else {
        size_bytes / ENTRY_SIZE as u64 + 1
    }
}

fn entry_proof(top_proof: &FlowProof, sub_proof: &FlowProof) -> Result<FlowProof> {
    if top_proof.item() != sub_proof.root() {
        bail!(
            "top tree and sub tree mismatch: top_leaf={:?}, sub_root={:?}",
            top_proof.item(),
            sub_proof.root()
        );
    }
    let mut lemma = sub_proof.lemma().to_vec();
    let mut path = sub_proof.path().to_vec();
    assert!(lemma.pop().is_some());
    lemma.extend_from_slice(&top_proof.lemma()[1..]);
    path.extend_from_slice(top_proof.path());
    FlowProof::new(lemma, path)
}

pub fn split_nodes(data_size: usize) -> Vec<usize> {
    let (mut padded_chunks, chunks_next_pow2) = compute_padded_chunk_size(data_size);
    let mut next_chunk_size = chunks_next_pow2;

    let mut nodes = vec![];
    while padded_chunks > 0 {
        if padded_chunks >= next_chunk_size {
            padded_chunks -= next_chunk_size;
            nodes.push(next_chunk_size);
        }

        next_chunk_size >>= 1;
    }

    nodes
}

pub fn tx_subtree_root_list_padded(data: &[u8]) -> Vec<(usize, DataRoot)> {
    let mut root_list = Vec::new();
    let mut start_index = 0;
    let nodes = split_nodes(data.len());

    for &tree_size in nodes.iter() {
        let end = start_index + tree_size * ENTRY_SIZE;

        let submerkle_root = if start_index >= data.len() {
            sub_merkle_tree(&vec![0u8; tree_size * ENTRY_SIZE])
                .unwrap()
                .root()
        } else if end > data.len() {
            let mut pad_data = data[start_index..].to_vec();
            pad_data.append(&mut vec![0u8; end - data.len()]);
            sub_merkle_tree(&pad_data).unwrap().root()
        } else {
            sub_merkle_tree(&data[start_index..end]).unwrap().root()
        };

        root_list.push((log2_pow2(tree_size) + 1, submerkle_root.into()));
        start_index = end;
    }

    root_list
}

pub fn sector_to_segment(sector_index: u64) -> usize {
    (sector_index / PORA_CHUNK_SIZE as u64) as usize
}

pub fn segment_to_sector(segment_index: usize) -> usize {
    segment_index * PORA_CHUNK_SIZE
}
