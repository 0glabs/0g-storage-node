use super::api::RpcServer;
use crate::error;
use crate::types::{FileInfo, Segment, SegmentWithProof, Status};
use crate::Context;
use chunk_pool::{FileID, SegmentInfo};
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use shared_types::{DataRoot, FlowProof, Transaction, CHUNK_SIZE};
use std::fmt::{Debug, Formatter, Result};
use storage::config::ShardConfig;
use storage::try_option;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn get_status(&self) -> RpcResult<Status> {
        info!("zgs_getStatus()");
        let sync_progress = self
            .ctx
            .log_store
            .get_store()
            .get_sync_progress()?
            .unwrap_or_default();

        Ok(Status {
            connected_peers: self.ctx.network_globals.connected_peers(),
            log_sync_height: sync_progress.0,
            log_sync_block: sync_progress.1,
        })
    }

    async fn upload_segment(&self, segment: SegmentWithProof) -> RpcResult<()> {
        info!(root = %segment.root, index = %segment.index, "zgs_uploadSegment");
        self.put_segment(segment).await
    }

    async fn upload_segments(&self, segments: Vec<SegmentWithProof>) -> RpcResult<()> {
        let root = match segments.first() {
            None => return Ok(()),
            Some(seg) => seg.root,
        };
        let indices = SegmentIndexArray::new(&segments);
        info!(%root, ?indices, "zgs_uploadSegments");

        for segment in segments.into_iter() {
            self.put_segment(segment).await?;
        }

        Ok(())
    }

    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>> {
        info!(%data_root, %start_index, %end_index, "zgs_downloadSegment");

        if start_index >= end_index {
            return Err(error::invalid_params("end_index", "invalid chunk index"));
        }

        if end_index - start_index > self.ctx.config.chunks_per_segment {
            return Err(error::invalid_params(
                "end_index",
                format!(
                    "exceeds maximum chunks {}",
                    self.ctx.config.chunks_per_segment
                ),
            ));
        }

        let tx_seq = try_option!(
            self.ctx
                .log_store
                .get_tx_seq_by_data_root(&data_root)
                .await?
        );
        let segment = try_option!(
            self.ctx
                .log_store
                .get_chunks_by_tx_and_index_range(tx_seq, start_index, end_index)
                .await?
        );

        Ok(Some(Segment(segment.data)))
    }

    async fn download_segment_with_proof(
        &self,
        data_root: DataRoot,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>> {
        info!(%data_root, %index, "zgs_downloadSegmentWithProof");

        let tx = try_option!(self.ctx.log_store.get_tx_by_data_root(&data_root).await?);

        // validate index
        let chunks_per_segment = self.ctx.config.chunks_per_segment;
        let (num_segments, last_segment_size) =
            SegmentWithProof::split_file_into_segments(tx.size as usize, chunks_per_segment)?;

        if index >= num_segments {
            return Err(error::invalid_params("index", "index out of bound"));
        }

        // calculate chunk start and end index
        let start_index = index * chunks_per_segment;
        let end_index = if index == num_segments - 1 {
            // last segment without padding chunks by flow
            start_index + last_segment_size / CHUNK_SIZE
        } else {
            start_index + chunks_per_segment
        };

        let segment = try_option!(
            self.ctx
                .log_store
                .get_chunks_with_proof_by_tx_and_index_range(tx.seq, start_index, end_index, None)
                .await?
        );

        let proof = tx.compute_segment_proof(&segment, chunks_per_segment)?;

        Ok(Some(SegmentWithProof {
            root: data_root,
            data: segment.chunks.data,
            index,
            proof,
            file_size: tx.size as usize,
        }))
    }

    async fn get_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>> {
        debug!(%data_root, "zgs_getFileInfo");

        let tx = try_option!(self.ctx.log_store.get_tx_by_data_root(&data_root).await?);

        Ok(Some(self.get_file_info_by_tx(tx).await?))
    }

    async fn get_file_info_by_tx_seq(&self, tx_seq: u64) -> RpcResult<Option<FileInfo>> {
        debug!(%tx_seq, "zgs_getFileInfoByTxSeq");

        let tx = try_option!(self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?);

        Ok(Some(self.get_file_info_by_tx(tx).await?))
    }

    async fn get_shard_config(&self) -> RpcResult<ShardConfig> {
        debug!("zgs_getShardConfig");
        let shard_config = self.ctx.log_store.get_store().flow().get_shard_config();
        Ok(shard_config)
    }

    async fn get_sector_proof(
        &self,
        sector_index: u64,
        flow_root: Option<DataRoot>,
    ) -> RpcResult<FlowProof> {
        let proof = self
            .ctx
            .log_store
            .get_proof_at_root(flow_root, sector_index, 1)
            .await?;
        assert_eq!(proof.left_proof, proof.right_proof);
        Ok(proof.right_proof)
    }
}

impl RpcServerImpl {
    async fn check_need_cache(
        &self,
        maybe_tx: &Option<Transaction>,
        file_size: usize,
    ) -> RpcResult<bool> {
        if let Some(tx) = maybe_tx {
            if tx.size != file_size as u64 {
                return Err(error::invalid_params(
                    "file_size",
                    "segment file size not matched with tx file size",
                ));
            }

            // Transaction already finalized for the specified file data root.
            if self.ctx.log_store.check_tx_completed(tx.seq).await? {
                return Err(error::invalid_params(
                    "root",
                    "already uploaded and finalized",
                ));
            }

            Ok(false)
        } else {
            //Check whether file is small enough to cache in the system
            if file_size > self.ctx.config.max_cache_file_size {
                return Err(error::invalid_params(
                    "file_size",
                    "caching of large file when tx is unavailable is not supported",
                ));
            }

            Ok(true)
        }
    }

    async fn get_file_info_by_tx(&self, tx: Transaction) -> RpcResult<FileInfo> {
        let finalized = self.ctx.log_store.check_tx_completed(tx.seq).await?;
        let (uploaded_seg_num, is_cached) = match self
            .ctx
            .chunk_pool
            .get_uploaded_seg_num(&tx.data_merkle_root)
            .await
        {
            Some(v) => v,
            _ => (
                if finalized {
                    let chunks_per_segment = self.ctx.config.chunks_per_segment;
                    let (num_segments, _) = SegmentWithProof::split_file_into_segments(
                        tx.size as usize,
                        chunks_per_segment,
                    )?;
                    num_segments
                } else {
                    0
                },
                false,
            ),
        };

        Ok(FileInfo {
            tx,
            finalized,
            is_cached,
            uploaded_seg_num,
        })
    }

    async fn put_segment(&self, segment: SegmentWithProof) -> RpcResult<()> {
        debug!(root = %segment.root, index = %segment.index, "putSegment");

        self.ctx.chunk_pool.validate_segment_size(&segment.data)?;

        let maybe_tx = self
            .ctx
            .log_store
            .get_tx_by_data_root(&segment.root)
            .await?;
        let mut need_cache = false;

        if self
            .ctx
            .chunk_pool
            .check_already_has_cache(&segment.root)
            .await
        {
            need_cache = true;
        }

        if !need_cache {
            need_cache = self.check_need_cache(&maybe_tx, segment.file_size).await?;
        }

        segment.validate(self.ctx.config.chunks_per_segment)?;

        let seg_info = SegmentInfo {
            root: segment.root,
            seg_data: segment.data,
            seg_proof: segment.proof,
            seg_index: segment.index,
            chunks_per_segment: self.ctx.config.chunks_per_segment,
        };

        if need_cache {
            self.ctx.chunk_pool.cache_chunks(seg_info).await?;
        } else {
            let file_id = FileID {
                root: seg_info.root,
                tx_id: maybe_tx.unwrap().id(),
            };
            self.ctx
                .chunk_pool
                .write_chunks(seg_info, file_id, segment.file_size)
                .await?;
        }
        Ok(())
    }
}

enum SegmentIndex {
    Single(usize),
    Range(usize, usize), // [start, end]
}

impl Debug for SegmentIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::Single(val) => write!(f, "{}", val),
            Self::Range(start, end) => write!(f, "[{},{}]", start, end),
        }
    }
}

struct SegmentIndexArray {
    items: Vec<SegmentIndex>,
}

impl Debug for SegmentIndexArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self.items.first() {
            None => write!(f, "NULL"),
            Some(first) if self.items.len() == 1 => write!(f, "{:?}", first),
            _ => write!(f, "{:?}", self.items),
        }
    }
}

impl SegmentIndexArray {
    fn new(segments: &[SegmentWithProof]) -> Self {
        let mut items = Vec::new();

        let mut current = match segments.first() {
            None => return SegmentIndexArray { items },
            Some(seg) => SegmentIndex::Single(seg.index),
        };

        for index in segments.iter().skip(1).map(|seg| seg.index) {
            match current {
                SegmentIndex::Single(val) if val + 1 == index => {
                    current = SegmentIndex::Range(val, index)
                }
                SegmentIndex::Range(start, end) if end + 1 == index => {
                    current = SegmentIndex::Range(start, index)
                }
                _ => {
                    items.push(current);
                    current = SegmentIndex::Single(index);
                }
            }
        }

        items.push(current);

        SegmentIndexArray { items }
    }
}
