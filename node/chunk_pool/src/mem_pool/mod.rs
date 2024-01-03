mod chunk_cache;
mod chunk_pool_inner;
mod chunk_write_control;

pub use chunk_pool_inner::MemoryChunkPool;
pub use chunk_pool_inner::SegmentInfo;

use shared_types::DataRoot;
use shared_types::TxID;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FileID {
    pub root: DataRoot,
    pub tx_id: TxID,
}
