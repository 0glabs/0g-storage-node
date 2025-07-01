use crate::zgs_grpc_proto::{PingRequest, PingReply, UploadSegmentsByTxSeqRequest, Empty};
use crate::zgs_grpc_proto::zgs_grpc_service_server::ZgsGrpcService;
use crate::{rpc_helper, Context, SegmentIndexArray};
use crate::types::SegmentWithProof as RpcSegment;

pub struct ZgsGrpcServiceImpl {
    pub ctx: Context,
}

#[tonic::async_trait]
impl ZgsGrpcService for ZgsGrpcServiceImpl {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingReply>, tonic::Status> {
        let msg = request.into_inner().message;
        let reply = PingReply { message: format!("Echo: {}", msg) };
        Ok(tonic::Response::new(reply))
    }

    async fn upload_segments_by_tx_seq(
        &self,
        request: tonic::Request<UploadSegmentsByTxSeqRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let req = request.into_inner();
        let segments = req.segments;
        let tx_seq = req.tx_seq;

        let rpc_segments: Vec<RpcSegment> = segments
            .into_iter()
            .map(RpcSegment::try_from)
            .collect::<Result<_, _>>()?;

        let indices = SegmentIndexArray::new(&rpc_segments);
        info!(%tx_seq, ?indices, "grpc_zgs_uploadSegmentsByTxSeq");

        let maybe_tx = self.ctx.log_store.get_tx_by_seq_number(tx_seq).await.map_err(|e| {
            tonic::Status::internal(format!("Failed to get transaction by sequence number: {}", e))
        })?;
        for segment in rpc_segments.into_iter() {
            rpc_helper::put_segment_with_maybe_tx(&self.ctx, segment, maybe_tx.clone())
                .await.map_err(|e| {
                    tonic::Status::internal(format!("Failed to put segment: {}", e))
                })?;
        }

        // Return an empty response
        Ok(tonic::Response::new(Empty {}))
    }
}
