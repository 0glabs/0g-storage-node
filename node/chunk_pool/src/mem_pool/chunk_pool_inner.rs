use super::chunk_cache::{ChunkPoolCache, MemoryCachedFile};
use super::chunk_write_control::ChunkPoolWriteCtrl;
use super::FileID;
use crate::handler::ChunkPoolMessage;
use crate::Config;
use anyhow::{anyhow, bail, Result};
use async_lock::Mutex;
use log_entry_sync::LogSyncEvent;
use shared_types::{
    bytes_to_chunks, compute_segment_size, ChunkArray, DataRoot, FileProof, Transaction, CHUNK_SIZE,
};
use std::sync::Arc;
use storage_async::{ShardConfig, Store};
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tokio::sync::mpsc::UnboundedSender;

struct Inner {
    config: Config,
    segment_cache: ChunkPoolCache,
    write_control: ChunkPoolWriteCtrl,
}

impl Inner {
    fn new(config: Config) -> Self {
        Inner {
            config,
            segment_cache: ChunkPoolCache::new(config),
            write_control: ChunkPoolWriteCtrl::new(config),
        }
    }

    fn after_flush_cache(&mut self) {
        assert!(self.write_control.total_writings > 0);
        self.write_control.total_writings -= 1;
    }

    /// Return the tx seq and all segments that belong to the root.
    fn get_all_cached_segments_to_write(
        &mut self,
        root: &DataRoot,
    ) -> Result<(FileID, Vec<(ChunkArray, FileProof)>)> {
        // Limits the number of writing threads.
        if self.write_control.total_writings >= self.config.max_writings {
            bail!("too many data writing: {}", self.config.max_writings);
        }

        let file = match self.segment_cache.remove_file(root) {
            Some(f) => f,
            None => bail!("file not found to write into store {:?}", root),
        };
        let id = file.id;
        let segs = file.segments.into_values().collect();

        self.write_control.total_writings += 1;

        Ok((id, segs))
    }
}

pub struct SegmentInfo {
    pub root: DataRoot,
    pub seg_data: Vec<u8>,
    pub seg_proof: FileProof,
    pub seg_index: usize,
    pub chunks_per_segment: usize,
}

impl From<SegmentInfo> for (ChunkArray, FileProof) {
    fn from(seg_info: SegmentInfo) -> Self {
        let start_index = seg_info.seg_index * seg_info.chunks_per_segment;
        (
            ChunkArray {
                data: seg_info.seg_data,
                start_index: start_index as u64,
            },
            seg_info.seg_proof,
        )
    }
}

/// Caches data chunks in memory before the entire file uploaded to storage node
/// and data root verified on blockchain.
pub struct MemoryChunkPool {
    inner: Mutex<Inner>,
    log_store: Arc<Store>,
    sender: UnboundedSender<ChunkPoolMessage>,
}

impl MemoryChunkPool {
    pub(crate) fn new(
        config: Config,
        log_store: Arc<Store>,
        sender: UnboundedSender<ChunkPoolMessage>,
    ) -> Self {
        MemoryChunkPool {
            inner: Mutex::new(Inner::new(config)),
            log_store,
            sender,
        }
    }

    pub fn validate_segment_size(&self, segment: &[u8]) -> Result<()> {
        if segment.is_empty() {
            bail!("data is empty");
        }

        if segment.len() % CHUNK_SIZE != 0 {
            bail!("invalid data length");
        }

        Ok(())
    }

    pub async fn cache_chunks(&self, seg_info: SegmentInfo) -> Result<()> {
        let root = seg_info.root;
        debug!("cache_chunks, root={:?} index={}", root, seg_info.seg_index);
        let should_flush = self
            .inner
            .lock()
            .await
            .segment_cache
            .cache_segment(seg_info)?;

        // store and finalize the cached file if completed
        if should_flush {
            debug!("cache_chunk: flush cached chunks");
            self.write_all_cached_chunks_and_finalize(root).await?;
        }

        Ok(())
    }

    pub async fn write_chunks(
        &self,
        seg_info: SegmentInfo,
        file_id: FileID,
        file_size: usize,
    ) -> Result<()> {
        let total_chunks = bytes_to_chunks(file_size);

        debug!(
            "Begin to write segment, root={}, segment_size={}, segment_index={}",
            seg_info.root,
            seg_info.seg_data.len(),
            seg_info.seg_index,
        );

        //Write the segment in window
        let (total_segments, _) = compute_segment_size(total_chunks, seg_info.chunks_per_segment);
        let tx_start_index = self
            .log_store
            .get_tx_by_seq_number(file_id.tx_id.seq)
            .await?
            .ok_or(anyhow!("unexpected tx missing"))?
            .start_entry_index()
            / seg_info.chunks_per_segment as u64;
        self.inner.lock().await.write_control.write_segment(
            file_id,
            seg_info.seg_index,
            total_segments,
            tx_start_index as usize,
        )?;

        // Write memory cached segments into store.
        // TODO(qhz): error handling
        // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
        // 2. Put the incompleted segments back to memory pool.
        let seg = ChunkArray {
            data: seg_info.seg_data,
            start_index: (seg_info.seg_index * seg_info.chunks_per_segment) as u64,
        };

        match self
            .log_store
            .put_chunks_with_tx_hash(
                file_id.tx_id.seq,
                file_id.tx_id.hash,
                seg,
                Some(seg_info.seg_proof.try_into()?),
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.inner
                    .lock()
                    .await
                    .write_control
                    .on_write_failed(&seg_info.root, seg_info.seg_index);
                // remove the file if transaction reverted
                self.inner
                    .lock()
                    .await
                    .write_control
                    .remove_file(&seg_info.root);
                bail!("Transaction reverted, please upload again");
            }
            Err(e) => {
                self.inner
                    .lock()
                    .await
                    .write_control
                    .on_write_failed(&seg_info.root, seg_info.seg_index);
                return Err(e);
            }
        }

        let all_uploaded = self
            .inner
            .lock()
            .await
            .write_control
            .on_write_succeeded(&seg_info.root, seg_info.seg_index);

        // Notify to finalize transaction asynchronously.
        if all_uploaded {
            self.send_finalize_file(file_id).await?;
            debug!("Queue to finalize transaction for file {}", seg_info.root);
        }

        Ok(())
    }

    /// Updates the cached file info when log entry retrieved from blockchain.
    pub async fn update_file_info(&self, tx: &Transaction) -> Result<bool> {
        info!(
            "start to flush cached segments to log store. data root: {}, tx_seq:{}",
            tx.data_merkle_root, tx.seq
        );
        let maybe_file = self
            .inner
            .lock()
            .await
            .segment_cache
            .remove_file(&tx.data_merkle_root);
        if let Some(mut file) = maybe_file {
            file.update_with_tx(tx);
            for (seg_index, (seg, proof)) in file.segments.into_iter() {
                self.write_chunks(
                    SegmentInfo {
                        root: tx.data_merkle_root,
                        seg_data: seg.data,
                        seg_proof: proof,
                        seg_index,
                        chunks_per_segment: file.chunks_per_segment,
                    },
                    file.id,
                    file.total_chunks * CHUNK_SIZE,
                )
                .await?
            }
        }
        info!(
            "cached segments flushed to log store. data root: {}, tx_seq:{}",
            tx.data_merkle_root, tx.seq
        );
        Ok(true)
    }

    pub async fn monitor_log_entry(chunk_pool: Arc<Self>, mut receiver: Receiver<LogSyncEvent>) {
        info!("Start to monitor log entry");

        loop {
            match receiver.recv().await {
                Ok(LogSyncEvent::ReorgDetected { .. }) => {}
                Ok(LogSyncEvent::Reverted { .. }) => {}
                Ok(LogSyncEvent::TxSynced { tx }) => {
                    // This may take a while, so execute it asynchronously to ensure the
                    // channel will not be saturated.
                    let chunk_pool_cloned = chunk_pool.clone();
                    tokio::spawn(async move {
                        if let Err(e) = chunk_pool_cloned.update_file_info(&tx).await {
                            error!(
                                "Failed to update file info. tx seq={}, tx_root={}, error={}",
                                tx.seq, tx.data_merkle_root, e
                            );
                        }
                    });
                }
                Err(RecvError::Closed) => {
                    // program terminated
                    info!("Completed to monitor log entry");
                    return;
                }
                Err(RecvError::Lagged(lagged)) => {
                    error!(%lagged, "Lagged messages: (Lagged)");
                }
            }
        }
    }

    pub(crate) async fn remove_cached_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        self.inner.lock().await.segment_cache.remove_file(root)
    }

    pub(crate) async fn remove_file(&self, root: &DataRoot) -> bool {
        let mut inner = self.inner.lock().await;
        inner.segment_cache.remove_file(root).is_some()
            || inner.write_control.remove_file(root).is_some()
    }

    pub async fn check_already_has_cache(&self, root: &DataRoot) -> bool {
        self.inner
            .lock()
            .await
            .segment_cache
            .get_file(root)
            .is_some()
    }

    async fn write_all_cached_chunks_and_finalize(&self, root: DataRoot) -> Result<()> {
        let (file, mut segments_with_proof) = self
            .inner
            .lock()
            .await
            .get_all_cached_segments_to_write(&root)?;

        while let Some((seg, proof)) = segments_with_proof.pop() {
            // TODO(qhz): error handling
            // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
            // 2. Put the incompleted segments back to memory pool.
            match self
                .log_store
                .put_chunks_with_tx_hash(
                    file.tx_id.seq,
                    file.tx_id.hash,
                    seg,
                    Some(proof.try_into()?),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    self.inner.lock().await.after_flush_cache();
                    bail!("Transaction reverted, please upload again");
                }
                Err(e) => {
                    self.inner.lock().await.after_flush_cache();
                    return Err(e);
                }
            }
        }

        self.inner.lock().await.after_flush_cache();

        self.send_finalize_file(file).await?;

        Ok(())
    }

    pub async fn get_uploaded_seg_num(&self, root: &DataRoot) -> Option<(usize, bool)> {
        let inner = self.inner.lock().await;

        if let Some(file) = inner.segment_cache.get_file(root) {
            Some((file.segments.len(), true))
        } else {
            inner
                .write_control
                .get_file(root)
                .map(|file| (file.uploaded_seg_num(), false))
        }
    }

    async fn send_finalize_file(&self, file_id: FileID) -> Result<()> {
        if let Err(e) = self.sender.send(ChunkPoolMessage::FinalizeFile(file_id)) {
            // Channel receiver will not be dropped until program exit.
            bail!("channel send error: {}", e);
        }
        Ok(())
    }

    pub fn sender(&self) -> UnboundedSender<ChunkPoolMessage> {
        self.sender.clone()
    }

    pub async fn set_shard_config(&self, shard_config: ShardConfig) {
        let mut inner = self.inner.lock().await;
        if inner.config.shard_config != shard_config {
            inner.config.shard_config = shard_config;
            inner.write_control.update_shard_config(shard_config);
        }
    }
}
