use crate::error;
use crate::types::SegmentWithProof;
use crate::Context;
use chunk_pool::SegmentInfo;
use jsonrpsee::core::RpcResult;
use shared_types::Transaction;

/// Put a single segment (mirrors your old `put_segment`)
pub async fn put_segment(ctx: &Context, segment: SegmentWithProof) -> RpcResult<()> {
    debug!(root = %segment.root, index = %segment.index, "putSegment");

    // fetch optional tx
    let maybe_tx = ctx
        .log_store
        .get_tx_by_data_root(&segment.root, false)
        .await?;

    put_segment_with_maybe_tx(ctx, segment, maybe_tx).await
}

/// Put a segment, given an optional Transaction (mirrors `put_segment_with_maybe_tx`)
pub async fn put_segment_with_maybe_tx(
    ctx: &Context,
    segment: SegmentWithProof,
    maybe_tx: Option<Transaction>,
) -> RpcResult<()> {
    ctx.chunk_pool.validate_segment_size(&segment.data)?;

    if let Some(tx) = &maybe_tx {
        if tx.data_merkle_root != segment.root {
            return Err(error::internal_error("data root and tx seq not match"));
        }
    }

    // decide cache vs write
    let need_cache = if ctx.chunk_pool.check_already_has_cache(&segment.root).await {
        true
    } else {
        check_need_cache(ctx, &maybe_tx, segment.file_size).await?
    };

    segment.validate(ctx.config.chunks_per_segment)?;

    let seg_info = SegmentInfo {
        root: segment.root,
        seg_data: segment.data,
        seg_proof: segment.proof,
        seg_index: segment.index,
        chunks_per_segment: ctx.config.chunks_per_segment,
    };

    if need_cache {
        ctx.chunk_pool.cache_chunks(seg_info).await?;
    } else {
        let file_id = chunk_pool::FileID {
            root: seg_info.root,
            tx_id: maybe_tx.unwrap().id(),
        };
        ctx.chunk_pool
            .write_chunks(seg_info, file_id, segment.file_size)
            .await?;
    }

    Ok(())
}

/// The old `check_need_cache`
pub async fn check_need_cache(
    ctx: &Context,
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
        if ctx.log_store.check_tx_completed(tx.seq).await? {
            return Err(error::invalid_params(
                "root",
                "already uploaded and finalized",
            ));
        }
        if ctx.log_store.check_tx_pruned(tx.seq).await? {
            return Err(error::invalid_params("root", "already pruned"));
        }
        Ok(false)
    } else {
        if file_size > ctx.config.max_cache_file_size {
            return Err(error::invalid_params(
                "file_size",
                "caching of large file when tx is unavailable is not supported",
            ));
        }
        Ok(true)
    }
}
