use async_trait::async_trait;
use storage::log_store::MineLoadChunk;
use storage_async::Store;

#[async_trait]
pub trait PoraLoader: Send + Sync {
    async fn load_sealed_data(&self, index: u64) -> Option<MineLoadChunk>;
}

#[async_trait]
impl PoraLoader for Store {
    async fn load_sealed_data(&self, chunk_index: u64) -> Option<MineLoadChunk> {
        match self.load_sealed_data(chunk_index).await {
            Ok(Some(chunk)) => Some(chunk),
            _ => None,
        }
    }
}
