use super::mem_pool::MemoryChunkPool;
use crate::mem_pool::FileID;
use anyhow::Result;
use network::NetworkMessage;
use shared_types::{ChunkArray, FileProof};
use std::sync::Arc;
use storage_async::Store;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Handle the cached file when uploaded completely and verified from blockchain.
/// Generally, the file will be persisted into log store.
pub struct ChunkPoolHandler {
    receiver: UnboundedReceiver<FileID>,
    mem_pool: Arc<MemoryChunkPool>,
    log_store: Store,
    sender: UnboundedSender<NetworkMessage>,
}

impl ChunkPoolHandler {
    pub(crate) fn new(
        receiver: UnboundedReceiver<FileID>,
        mem_pool: Arc<MemoryChunkPool>,
        log_store: Store,
        sender: UnboundedSender<NetworkMessage>,
    ) -> Self {
        ChunkPoolHandler {
            receiver,
            mem_pool,
            log_store,
            sender,
        }
    }

    /// Writes memory cached chunks into store and finalize transaction.
    /// Note, a separate thread should be spawned to call this method.
    pub async fn handle(&mut self) -> Result<bool> {
        let id = match self.receiver.recv().await {
            Some(id) => id,
            None => return Ok(false),
        };

        debug!(?id, "Received task to finalize transaction");

        // TODO(qhz): remove from memory pool after transaction finalized,
        // when store support to write chunks with reference.
        if let Some(file) = self.mem_pool.remove_cached_file(&id.root).await {
            // If there is still cache of chunks, write them into store
            let mut segments: Vec<(ChunkArray, FileProof)> = file.segments.into_values().collect();
            while let Some((seg, proof)) = segments.pop() {
                if !self
                    .log_store
                    .put_chunks_with_tx_hash(
                        id.tx_id.seq,
                        id.tx_id.hash,
                        seg,
                        Some(proof.try_into()?),
                    )
                    .await?
                {
                    return Ok(false);
                }
            }
        }

        if !self
            .log_store
            .finalize_tx_with_hash(id.tx_id.seq, id.tx_id.hash)
            .await?
        {
            return Ok(false);
        }

        debug!(?id, "Transaction finalized");

        // always remove file from pool after transaction finalized
        self.mem_pool.remove_file(&id.root).await;

        let msg = NetworkMessage::AnnounceLocalFile { tx_id: id.tx_id };
        if let Err(e) = self.sender.send(msg) {
            error!(
                "Failed to send NetworkMessage::AnnounceLocalFile message, tx_seq={}, err={}",
                id.tx_id.seq, e
            );
        }

        Ok(true)
    }

    pub async fn run(mut self) {
        info!("Worker started to finalize transactions");

        loop {
            if let Err(e) = self.handle().await {
                warn!("Failed to write chunks or finalize transaction, {:?}", e);
            }
        }
    }
}
