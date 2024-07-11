use crate::config::ShardConfig;
use append_merkle::MerkleTreeInitialData;
use ethereum_types::H256;
use shared_types::{
    Chunk, ChunkArray, ChunkArrayWithProof, ChunkWithProof, DataRoot, FlowProof, FlowRangeProof,
    Transaction,
};
use zgs_spec::{BYTES_PER_SEAL, SEALS_PER_LOAD};

use crate::error::Result;

use self::tx_store::BlockHashAndSubmissionIndex;

pub mod config;
mod flow_store;
mod load_chunk;
pub mod log_manager;
#[cfg(test)]
mod tests;
pub mod tx_store;

/// The trait to read the transactions already appended to the log.
///
/// Implementation Rationale:
/// If the stored chunk is large, we can store the proof together with the chunk.
pub trait LogStoreRead: LogStoreChunkRead {
    /// Get a transaction by its global log sequence number.
    fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<Transaction>>;

    /// Get a transaction by the data root of its data.
    fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> Result<Option<u64>>;

    fn get_tx_by_data_root(&self, data_root: &DataRoot) -> Result<Option<Transaction>> {
        match self.get_tx_seq_by_data_root(data_root)? {
            Some(seq) => self.get_tx_by_seq_number(seq),
            None => Ok(None),
        }
    }

    fn get_chunk_with_proof_by_tx_and_index(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> Result<Option<ChunkWithProof>>;

    fn get_chunks_with_proof_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
        merkle_tx_seq: Option<u64>,
    ) -> Result<Option<ChunkArrayWithProof>>;

    fn check_tx_completed(&self, tx_seq: u64) -> Result<bool>;

    fn next_tx_seq(&self) -> u64;

    fn get_sync_progress(&self) -> Result<Option<(u64, H256)>>;

    fn get_block_hash_by_number(&self, block_number: u64) -> Result<Option<(H256, Option<u64>)>>;

    fn get_block_hashes(&self) -> Result<Vec<(u64, BlockHashAndSubmissionIndex)>>;

    fn validate_range_proof(&self, tx_seq: u64, data: &ChunkArrayWithProof) -> Result<bool>;

    fn get_proof_at_root(
        &self,
        root: Option<DataRoot>,
        index: u64,
        length: u64,
    ) -> Result<FlowRangeProof>;

    /// Return flow root and length.
    fn get_context(&self) -> Result<(DataRoot, u64)>;
}

pub trait LogStoreChunkRead {
    /// Get a data chunk by the transaction sequence number and the chunk offset in the transaction.
    /// Accessing a single chunk is mostly used for mining.
    fn get_chunk_by_tx_and_index(&self, tx_seq: u64, index: usize) -> Result<Option<Chunk>>;

    /// Get a list of continuous chunks by the transaction sequence number and an index range (`index_end` excluded).
    fn get_chunks_by_tx_and_index_range(
        &self,
        tx_seq: u64,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>>;

    fn get_chunk_by_data_root_and_index(
        &self,
        data_root: &DataRoot,
        index: usize,
    ) -> Result<Option<Chunk>>;

    fn get_chunks_by_data_root_and_index_range(
        &self,
        data_root: &DataRoot,
        index_start: usize,
        index_end: usize,
    ) -> Result<Option<ChunkArray>>;

    fn get_chunk_index_list(&self, tx_seq: u64) -> Result<Vec<usize>>;

    /// Accessing chunks by absolute flow index
    fn get_chunk_by_flow_index(&self, index: u64, length: u64) -> Result<Option<ChunkArray>>;
}

pub trait LogStoreWrite: LogStoreChunkWrite {
    /// Store a data entry metadata.
    fn put_tx(&self, tx: Transaction) -> Result<()>;

    /// Finalize a transaction storage.
    /// This will compute and the merkle tree, check the data root, and persist a part of the merkle
    /// tree for future queries.
    ///
    /// This will return error if not all chunks are stored. But since this check can be expensive,
    /// the caller is supposed to track chunk statuses and call this after storing all the chunks.
    fn finalize_tx(&self, tx_seq: u64) -> Result<()>;
    fn finalize_tx_with_hash(&self, tx_seq: u64, tx_hash: H256) -> Result<bool>;

    /// Store the progress of synced block number and its hash.
    fn put_sync_progress(&self, progress: (u64, H256, Option<Option<u64>>)) -> Result<()>;

    /// Revert the log state to a given tx seq.
    /// This is needed when transactions are reverted because of chain reorg.
    ///
    /// Reverted transactions are returned in order.
    fn revert_to(&self, tx_seq: u64) -> Result<Vec<Transaction>>;

    /// If the proof is valid, fill the tree nodes with the new data.
    fn validate_and_insert_range_proof(
        &self,
        tx_seq: u64,
        data: &ChunkArrayWithProof,
    ) -> Result<bool>;

    fn delete_block_hash_by_number(&self, block_number: u64) -> Result<()>;
}

pub trait LogStoreChunkWrite {
    /// Store data chunks of a data entry.
    fn put_chunks(&self, tx_seq: u64, chunks: ChunkArray) -> Result<()>;

    fn put_chunks_with_tx_hash(
        &self,
        tx_seq: u64,
        tx_hash: H256,
        chunks: ChunkArray,
        maybe_file_proof: Option<FlowProof>,
    ) -> Result<bool>;

    /// Delete a list of chunk batches from the db.
    /// `batch_list` is a `Vec` of entry batch index.
    fn remove_chunks_batch(&self, batch_list: &[u64]) -> Result<()>;
}

pub trait LogChunkStore: LogStoreChunkRead + LogStoreChunkWrite + Send + Sync + 'static {}
impl<T: LogStoreChunkRead + LogStoreChunkWrite + Send + Sync + 'static> LogChunkStore for T {}

pub trait Store:
    LogStoreRead + LogStoreWrite + LogStoreInner + config::Configurable + Send + Sync + 'static
{
}
impl<
        T: LogStoreRead + LogStoreWrite + LogStoreInner + config::Configurable + Send + Sync + 'static,
    > Store for T
{
}

pub trait LogStoreInner {
    fn flow(&self) -> &dyn Flow;
    fn flow_mut(&mut self) -> &mut dyn Flow;
}

pub struct MineLoadChunk {
    // Use `Vec` instead of array to avoid thread stack overflow.
    pub loaded_chunk: Vec<[u8; BYTES_PER_SEAL]>,
    pub avalibilities: [bool; SEALS_PER_LOAD],
}

impl Default for MineLoadChunk {
    fn default() -> Self {
        Self {
            loaded_chunk: vec![[0u8; BYTES_PER_SEAL]; SEALS_PER_LOAD],
            avalibilities: [false; SEALS_PER_LOAD],
        }
    }
}

pub trait FlowRead {
    /// Return the entries in the given range. If some data are missing, `Ok(None)` is returned.
    fn get_entries(&self, index_start: u64, index_end: u64) -> Result<Option<ChunkArray>>;

    /// Return the available entries in the given range.
    /// The `ChunkArray` in the returned list are in order and they will not overlap or be adjacent.
    ///
    /// For simplicity, `index_start` and `index_end` must be at the batch boundaries.
    fn get_available_entries(&self, index_start: u64, index_end: u64) -> Result<Vec<ChunkArray>>;

    fn get_chunk_root_list(&self) -> Result<MerkleTreeInitialData<DataRoot>>;

    fn load_sealed_data(&self, chunk_index: u64) -> Result<Option<MineLoadChunk>>;

    // An estimation of the number of entries in the flow db.
    fn get_num_entries(&self) -> Result<u64>;

    fn get_shard_config(&self) -> ShardConfig;
}

pub trait FlowWrite {
    /// Append data to the flow. `start_index` is included in `ChunkArray`, so
    /// it's possible to append arrays in any place.
    /// Return the list of completed chunks.
    fn append_entries(&self, data: ChunkArray) -> Result<Vec<(u64, DataRoot)>>;

    /// Remove all the entries after `start_index`.
    /// This is used to remove deprecated data in case of chain reorg.
    fn truncate(&self, start_index: u64) -> Result<()>;

    /// Update the shard config.
    fn update_shard_config(&self, shard_config: ShardConfig);
}

pub struct SealTask {
    /// The index (in seal) of chunks
    pub seal_index: u64,
    /// An ephemeral version number to distinguish if revert happending
    pub version: usize,
    /// The data to be sealed
    pub non_sealed_data: [u8; BYTES_PER_SEAL],
}

#[derive(Debug)]
pub struct SealAnswer {
    /// The index (in seal) of chunks
    pub seal_index: u64,
    /// An ephemeral version number to distinguish if revert happending
    pub version: usize,
    /// The data to be sealed
    pub sealed_data: [u8; BYTES_PER_SEAL],
    /// The miner Id
    pub miner_id: H256,
    /// The seal_context for this chunk
    pub seal_context: H256,
    pub context_end_seal: u64,
}

pub trait FlowSeal {
    /// Pull a seal chunk ready for sealing
    /// Return the global index (in sector) and the data
    fn pull_seal_chunk(&self, seal_index_max: usize) -> Result<Option<Vec<SealTask>>>;

    /// Submit sealing result

    fn submit_seal_result(&self, answers: Vec<SealAnswer>) -> Result<()>;
}

pub trait Flow: FlowRead + FlowWrite + FlowSeal {}
impl<T: FlowRead + FlowWrite + FlowSeal> Flow for T {}
