use super::methods::*;
use super::protocol::Protocol;
use super::protocol::ProtocolId;
use super::RPCError;
use crate::rpc::protocol::Encoding;
use crate::rpc::protocol::Version;
use crate::rpc::{
    codec::{base::BaseOutboundCodec, ssz_snappy::SSZSnappyOutboundCodec, OutboundCodec},
    methods::ResponseTermination,
};
use futures::future::BoxFuture;
use futures::prelude::{AsyncRead, AsyncWrite};
use futures::{FutureExt, SinkExt};
use libp2p::core::{OutboundUpgrade, UpgradeInfo};
use tokio_util::{
    codec::Framed,
    compat::{Compat, FuturesAsyncReadCompatExt},
};

/* Outbound request */

// Combines all the RPC requests into a single enum to implement `UpgradeInfo` and
// `OutboundUpgrade`

#[derive(Debug, Clone)]
pub struct OutboundRequestContainer {
    pub req: OutboundRequest,
    pub max_rpc_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRequest {
    Status(StatusMessage),
    Goodbye(GoodbyeReason),
    Ping(Ping),
    DataByHash(DataByHashRequest),
    GetChunks(GetChunksRequest),
}

impl UpgradeInfo for OutboundRequestContainer {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    // add further protocols as we support more encodings/versions
    fn protocol_info(&self) -> Self::InfoIter {
        self.req.supported_protocols()
    }
}

/// Implements the encoding per supported protocol for `RPCRequest`.
impl OutboundRequest {
    pub fn supported_protocols(&self) -> Vec<ProtocolId> {
        match self {
            // add more protocols when versions/encodings are supported
            OutboundRequest::Status(_) => vec![ProtocolId::new(
                Protocol::Status,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            OutboundRequest::Goodbye(_) => vec![ProtocolId::new(
                Protocol::Goodbye,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            OutboundRequest::Ping(_) => vec![ProtocolId::new(
                Protocol::Ping,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            OutboundRequest::DataByHash(_) => vec![ProtocolId::new(
                Protocol::DataByHash,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            OutboundRequest::GetChunks(_) => vec![ProtocolId::new(
                Protocol::GetChunks,
                Version::V1,
                Encoding::SSZSnappy,
            )],
        }
    }

    /* These functions are used in the handler for stream management */

    /// Number of responses expected for this request.
    pub fn expected_responses(&self) -> u64 {
        match self {
            OutboundRequest::Status(_) => 1,
            OutboundRequest::Goodbye(_) => 0,
            OutboundRequest::Ping(_) => 1,
            OutboundRequest::DataByHash(req) => req.hashes.len() as u64,
            OutboundRequest::GetChunks(_) => 1,
        }
    }

    /// Gives the corresponding `Protocol` to this request.
    pub fn protocol(&self) -> Protocol {
        match self {
            OutboundRequest::Status(_) => Protocol::Status,
            OutboundRequest::Goodbye(_) => Protocol::Goodbye,
            OutboundRequest::Ping(_) => Protocol::Ping,
            OutboundRequest::DataByHash(_) => Protocol::DataByHash,
            OutboundRequest::GetChunks(_) => Protocol::GetChunks,
        }
    }

    /// Returns the `ResponseTermination` type associated with the request if a stream gets
    /// terminated.
    pub fn stream_termination(&self) -> ResponseTermination {
        match self {
            // this only gets called after `multiple_responses()` returns true. Therefore, only
            // variants that have `multiple_responses()` can have values.
            OutboundRequest::DataByHash(_) => ResponseTermination::DataByHash,
            OutboundRequest::Status(_) => unreachable!(),
            OutboundRequest::Goodbye(_) => unreachable!(),
            OutboundRequest::Ping(_) => unreachable!(),
            OutboundRequest::GetChunks(_) => unreachable!(),
        }
    }
}

/* RPC Response type - used for outbound upgrades */

/* Outbound upgrades */

pub type OutboundFramed<TSocket> = Framed<Compat<TSocket>, OutboundCodec>;

impl<TSocket> OutboundUpgrade<TSocket> for OutboundRequestContainer
where
    TSocket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = OutboundFramed<TSocket>;
    type Error = RPCError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, socket: TSocket, protocol: Self::Info) -> Self::Future {
        // convert to a tokio compatible socket
        let socket = socket.compat();
        let codec = match protocol.encoding {
            Encoding::SSZSnappy => {
                let ssz_snappy_codec = BaseOutboundCodec::new(SSZSnappyOutboundCodec::new(
                    protocol,
                    self.max_rpc_size,
                ));
                OutboundCodec::SSZSnappy(ssz_snappy_codec)
            }
        };

        let mut socket = Framed::new(socket, codec);

        async {
            socket.send(self.req).await?;
            socket.close().await?;
            Ok(socket)
        }
        .boxed()
    }
}

impl std::fmt::Display for OutboundRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboundRequest::Status(status) => {
                write!(f, "Status Message: {}", status)
            }
            OutboundRequest::Goodbye(reason) => {
                write!(f, "Goodbye: {}", reason)
            }
            OutboundRequest::Ping(ping) => write!(f, "Ping: {}", ping.data),
            OutboundRequest::DataByHash(req) => {
                write!(f, "Data by hash: {:?}", req)
            }
            OutboundRequest::GetChunks(req) => {
                write!(f, "GetChunks: {:?}", req)
            }
        }
    }
}
