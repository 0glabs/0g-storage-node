use super::api::RpcServer;
use crate::types::{FileInfo, Segment, SegmentWithProof, Status};
use crate::Context;
use crate::{error, rpc_helper, SegmentIndexArray};
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use shared_types::{DataRoot, FlowProof, Transaction, TxSeqOrRoot, CHUNK_SIZE};
use storage::config::ShardConfig;
use storage::log_store::tx_store::TxStatus;
use storage::{try_option, H256};

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn get_status(&self) -> RpcResult<Status> {
        info!("zgs_getStatus()");
        let sync_progress = self
            .ctx
            .log_store
            .get_store()
            .get_sync_progress()?
            .unwrap_or_default();

        let next_tx_seq = self.ctx.log_store.get_store().next_tx_seq();

        Ok(Status {
            connected_peers: self.ctx.network_globals.connected_peers(),
            log_sync_height: sync_progress.0,
            log_sync_block: sync_progress.1,
            next_tx_seq,
            network_identity: self.ctx.network_globals.network_id(),
        })
    }

    async fn upload_segment(&self, segment: SegmentWithProof) -> RpcResult<()> {
        info!(root = %segment.root, index = %segment.index, "zgs_uploadSegment");
        rpc_helper::put_segment(&self.ctx, segment).await
    }

    async fn upload_segment_by_tx_seq(
        &self,
        segment: SegmentWithProof,
        tx_seq: u64,
    ) -> RpcResult<()> {
        info!(tx_seq = %tx_seq, index = %segment.index, "zgs_uploadSegmentByTxSeq");
        let maybe_tx = self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?;
        rpc_helper::put_segment_with_maybe_tx(&self.ctx, segment, maybe_tx).await
    }

    async fn upload_segments(&self, segments: Vec<SegmentWithProof>) -> RpcResult<()> {
        let root = match segments.first() {
            None => return Ok(()),
            Some(seg) => seg.root,
        };
        let indices = SegmentIndexArray::new(&segments);
        info!(%root, ?indices, "zgs_uploadSegments");

        for segment in segments.into_iter() {
            rpc_helper::put_segment(&self.ctx, segment).await?;
        }

        Ok(())
    }

    async fn upload_segments_by_tx_seq(
        &self,
        segments: Vec<SegmentWithProof>,
        tx_seq: u64,
    ) -> RpcResult<()> {
        let indices = SegmentIndexArray::new(&segments);
        info!(%tx_seq, ?indices, "zgs_uploadSegmentsByTxSeq");

        let maybe_tx = self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?;
        for segment in segments.into_iter() {
            match rpc_helper::put_segment_with_maybe_tx(&self.ctx, segment, maybe_tx.clone()).await
            {
                Ok(()) => {} // success
                Err(e)
                    if e.to_string()
                        .contains("segment has already been uploaded or is being uploaded") =>
                {
                    debug!(?e, "duplicate segment - skipping");
                }
                Err(e) => return Err(e),
            }
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

        let tx_seq = try_option!(
            self.ctx
                .log_store
                .get_tx_seq_by_data_root(&data_root, true)
                .await?
        );

        self.get_segment_by_tx_seq(tx_seq, start_index, end_index)
            .await
    }

    async fn download_segment_by_tx_seq(
        &self,
        tx_seq: u64,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>> {
        info!(%tx_seq, %start_index, %end_index, "zgs_downloadSegmentByTxSeq");
        self.get_segment_by_tx_seq(tx_seq, start_index, end_index)
            .await
    }

    async fn download_segment_with_proof(
        &self,
        data_root: DataRoot,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>> {
        info!(%data_root, %index, "zgs_downloadSegmentWithProof");

        let tx = try_option!(
            self.ctx
                .log_store
                .get_tx_by_data_root(&data_root, true)
                .await?
        );

        self.get_segment_with_proof_by_tx(tx, index).await
    }

    async fn download_segment_with_proof_by_tx_seq(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>> {
        info!(%tx_seq, %index, "zgs_downloadSegmentWithProofByTxSeq");

        let tx = try_option!(self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?);

        self.get_segment_with_proof_by_tx(tx, index).await
    }

    async fn check_file_finalized(&self, tx_seq_or_root: TxSeqOrRoot) -> RpcResult<Option<bool>> {
        debug!(?tx_seq_or_root, "zgs_checkFileFinalized");

        let seq = match tx_seq_or_root {
            TxSeqOrRoot::TxSeq(v) => v,
            TxSeqOrRoot::Root(v) => {
                try_option!(
                    self.ctx
                        .log_store
                        .get_tx_seq_by_data_root(&v, false)
                        .await?
                )
            }
        };

        if self.ctx.log_store.check_tx_completed(seq).await? {
            Ok(Some(true))
        } else if self
            .ctx
            .log_store
            .get_tx_by_seq_number(seq)
            .await?
            .is_some()
        {
            Ok(Some(false))
        } else {
            Ok(None)
        }
    }

    async fn get_file_info(
        &self,
        data_root: DataRoot,
        need_available: bool,
    ) -> RpcResult<Option<FileInfo>> {
        debug!(%data_root, "zgs_getFileInfo");

        let tx = try_option!(
            self.ctx
                .log_store
                .get_tx_by_data_root(&data_root, need_available)
                .await?
        );

        Ok(Some(self.get_file_info_by_tx(tx).await?))
    }

    async fn get_file_info_by_tx_seq(&self, tx_seq: u64) -> RpcResult<Option<FileInfo>> {
        debug!(%tx_seq, "zgs_getFileInfoByTxSeq");

        let tx = try_option!(self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?);

        Ok(Some(self.get_file_info_by_tx(tx).await?))
    }

    async fn get_shard_config(&self) -> RpcResult<ShardConfig> {
        debug!("zgs_getShardConfig");
        let shard_config = self.ctx.log_store.get_store().get_shard_config();
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

    async fn get_flow_context(&self) -> RpcResult<(H256, u64)> {
        Ok(self.ctx.log_store.get_context().await?)
    }
}

impl RpcServerImpl {
    // async fn check_need_cache(
    //     &self,
    //     maybe_tx: &Option<Transaction>,
    //     file_size: usize,
    // ) -> RpcResult<bool> {
    //     if let Some(tx) = maybe_tx {
    //         if tx.size != file_size as u64 {
    //             return Err(error::invalid_params(
    //                 "file_size",
    //                 "segment file size not matched with tx file size",
    //             ));
    //         }

    //         // Transaction already finalized for the specified file data root.
    //         if self.ctx.log_store.check_tx_completed(tx.seq).await? {
    //             return Err(error::invalid_params(
    //                 "root",
    //                 "already uploaded and finalized",
    //             ));
    //         }

    //         if self.ctx.log_store.check_tx_pruned(tx.seq).await? {
    //             return Err(error::invalid_params("root", "already pruned"));
    //         }

    //         Ok(false)
    //     } else {
    //         //Check whether file is small enough to cache in the system
    //         if file_size > self.ctx.config.max_cache_file_size {
    //             return Err(error::invalid_params(
    //                 "file_size",
    //                 "caching of large file when tx is unavailable is not supported",
    //             ));
    //         }

    //         Ok(true)
    //     }
    // }

    async fn get_file_info_by_tx(&self, tx: Transaction) -> RpcResult<FileInfo> {
        let (finalized, pruned) = match self.ctx.log_store.get_store().get_tx_status(tx.seq)? {
            Some(TxStatus::Finalized) => (true, false),
            Some(TxStatus::Pruned) => (false, true),
            None => (false, false),
        };

        let (uploaded_seg_num, is_cached) = match self
            .ctx
            .chunk_pool
            .get_uploaded_seg_num(&tx.data_merkle_root)
            .await
        {
            Some(v) => v,
            _ => (
                if finalized || pruned {
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
            pruned,
        })
    }

    // async fn put_segment(&self, segment: SegmentWithProof) -> RpcResult<()> {
    //     debug!(root = %segment.root, index = %segment.index, "putSegment");

    //     let maybe_tx = self
    //         .ctx
    //         .log_store
    //         .get_tx_by_data_root(&segment.root, false)
    //         .await?;

    //     self.put_segment_with_maybe_tx(segment, maybe_tx).await
    // }

    // async fn put_segment_with_maybe_tx(
    //     &self,
    //     segment: SegmentWithProof,
    //     maybe_tx: Option<Transaction>,
    // ) -> RpcResult<()> {
    //     self.ctx.chunk_pool.validate_segment_size(&segment.data)?;

    //     if let Some(tx) = &maybe_tx {
    //         if tx.data_merkle_root != segment.root {
    //             return Err(error::internal_error("data root and tx seq not match"));
    //         }
    //     }

    //     let mut need_cache = false;
    //     if self
    //         .ctx
    //         .chunk_pool
    //         .check_already_has_cache(&segment.root)
    //         .await
    //     {
    //         need_cache = true;
    //     }

    //     if !need_cache {
    //         need_cache = self.check_need_cache(&maybe_tx, segment.file_size).await?;
    //     }

    //     segment.validate(self.ctx.config.chunks_per_segment)?;

    //     let seg_info = SegmentInfo {
    //         root: segment.root,
    //         seg_data: segment.data,
    //         seg_proof: segment.proof,
    //         seg_index: segment.index,
    //         chunks_per_segment: self.ctx.config.chunks_per_segment,
    //     };

    //     if need_cache {
    //         self.ctx.chunk_pool.cache_chunks(seg_info).await?;
    //     } else {
    //         let file_id = FileID {
    //             root: seg_info.root,
    //             tx_id: maybe_tx.unwrap().id(),
    //         };
    //         self.ctx
    //             .chunk_pool
    //             .write_chunks(seg_info, file_id, segment.file_size)
    //             .await?;
    //     }
    //     Ok(())
    // }

    async fn get_segment_by_tx_seq(
        &self,
        tx_seq: u64,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>> {
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

        let segment = try_option!(
            self.ctx
                .log_store
                .get_chunks_by_tx_and_index_range(tx_seq, start_index, end_index)
                .await?
        );

        Ok(Some(Segment(segment.data)))
    }

    async fn get_segment_with_proof_by_tx(
        &self,
        tx: Transaction,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>> {
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
            root: tx.data_merkle_root,
            data: segment.chunks.data,
            index,
            proof,
            file_size: tx.size as usize,
        }))
    }
}
