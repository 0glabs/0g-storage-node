use crate::zgs_grpc_proto::{PingRequest, PingReply};
use crate::zgs_grpc_proto::zgs_grpc_service_server::ZgsGrpcService;
use crate::Context;

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
}
