use crate::types::{FileInfo, Segment, SegmentWithProof, Status};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use shared_types::{DataRoot, FlowProof, TxSeqOrRoot};
use storage::{config::ShardConfig, H256};

#[rpc(server, client, namespace = "zgs")]
pub trait Rpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> RpcResult<Status>;

    #[method(name = "uploadSegment")]
    async fn upload_segment(&self, segment: SegmentWithProof) -> RpcResult<()>;

    #[method(name = "uploadSegmentByTxSeq")]
    async fn upload_segment_by_tx_seq(
        &self,
        segment: SegmentWithProof,
        tx_seq: u64,
    ) -> RpcResult<()>;

    #[method(name = "uploadSegments")]
    async fn upload_segments(&self, segments: Vec<SegmentWithProof>) -> RpcResult<()>;

    #[method(name = "uploadSegmentsByTxSeq")]
    async fn upload_segments_by_tx_seq(
        &self,
        segments: Vec<SegmentWithProof>,
        tx_seq: u64,
    ) -> RpcResult<()>;

    #[method(name = "downloadSegment")]
    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>>;

    #[method(name = "downloadSegmentByTxSeq")]
    async fn download_segment_by_tx_seq(
        &self,
        tx_seq: u64,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>>;

    #[method(name = "downloadSegmentWithProof")]
    async fn download_segment_with_proof(
        &self,
        data_root: DataRoot,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>>;

    #[method(name = "downloadSegmentWithProofByTxSeq")]
    async fn download_segment_with_proof_by_tx_seq(
        &self,
        tx_seq: u64,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>>;

    #[method(name = "checkFileFinalized")]
    async fn check_file_finalized(&self, tx_seq_or_root: TxSeqOrRoot) -> RpcResult<Option<bool>>;

    #[method(name = "getFileInfo")]
    async fn get_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>>;

    #[method(name = "getFirstAvailabelFileInfo")]
    async fn get_first_available_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>>;

    #[method(name = "getFileInfoByTxSeq")]
    async fn get_file_info_by_tx_seq(&self, tx_seq: u64) -> RpcResult<Option<FileInfo>>;

    #[method(name = "getShardConfig")]
    async fn get_shard_config(&self) -> RpcResult<ShardConfig>;

    #[method(name = "getSectorProof")]
    async fn get_sector_proof(
        &self,
        sector_index: u64,
        flow_root: Option<DataRoot>,
    ) -> RpcResult<FlowProof>;

    #[method(name = "getFlowContext")]
    async fn get_flow_context(&self) -> RpcResult<(H256, u64)>;
}
