use super::methods::*;
use crate::rpc::{
    codec::{base::BaseInboundCodec, ssz_snappy::SSZSnappyInboundCodec, InboundCodec},
    methods::{MaxErrorLen, ResponseTermination, MAX_CHUNKS_LENGTH, MAX_ERROR_LEN},
    MaxRequestBlocks, MAX_REQUEST_BLOCKS,
};
use futures::future::BoxFuture;
use futures::prelude::{AsyncRead, AsyncWrite};
use futures::{FutureExt, StreamExt};
use libp2p::core::{InboundUpgrade, ProtocolName, UpgradeInfo};
use shared_types::{ChunkArray, ChunkArrayWithProof, FlowRangeProof, ShardedFile};
use ssz::Encode;
use ssz_types::VariableList;
use std::io;
use std::time::Duration;
use strum::IntoStaticStr;
use tokio_io_timeout::TimeoutStream;
use tokio_util::{
    codec::Framed,
    compat::{Compat, FuturesAsyncReadCompatExt},
};

pub type Hash256 = ethereum_types::H256;

lazy_static! {
    pub static ref DATA_BY_HASH_REQUEST_MIN: usize =
        VariableList::<Hash256, MaxRequestBlocks>::from(Vec::<Hash256>::new())
            .as_ssz_bytes()
            .len();
    pub static ref DATA_BY_HASH_REQUEST_MAX: usize =
        VariableList::<Hash256, MaxRequestBlocks>::from(vec![
            Hash256::zero();
            MAX_REQUEST_BLOCKS as usize
        ])
        .as_ssz_bytes()
        .len();
    pub static ref ERROR_TYPE_MIN: usize = VariableList::<u8, MaxErrorLen>::from(Vec::<u8>::new())
        .as_ssz_bytes()
        .len();
    pub static ref ERROR_TYPE_MAX: usize =
        VariableList::<u8, MaxErrorLen>::from(vec![0u8; MAX_ERROR_LEN as usize])
            .as_ssz_bytes()
            .len();
    pub static ref CHUNKS_RESPONSE_MIN: usize = ChunkArrayWithProof {
        chunks: ChunkArray {
            data: vec![],
            start_index: 0,
        },
        proof: FlowRangeProof::new_empty(),
    }
    .as_ssz_bytes()
    .len();
    // TODO
    pub static ref CHUNKS_RESPONSE_MAX: usize = ChunkArrayWithProof {
        chunks: ChunkArray {
            data: vec![0u8; MAX_CHUNKS_LENGTH],
            start_index: 0,
        },
        proof: FlowRangeProof::new_empty(),
    }
    .as_ssz_bytes()
    .len();
}

// /// The maximum bytes that can be sent across the RPC pre-merge.
// pub(crate) const MAX_RPC_SIZE: usize = 1_048_576; // 1M
/// The maximum bytes that can be sent across the RPC post-merge.
pub(crate) const MAX_RPC_SIZE: usize = 10 * 1_048_576; // 10M
/// The protocol prefix the RPC protocol id.
const PROTOCOL_PREFIX: &str = "/zgs/req";
/// Time allowed for the first byte of a request to arrive before we time out (Time To First Byte).
const TTFB_TIMEOUT: u64 = 5;
/// The number of seconds to wait for the first bytes of a request once a protocol has been
/// established before the stream is terminated.
const REQUEST_TIMEOUT: u64 = 15;

/// Returns the maximum bytes that can be sent across the RPC.
pub fn max_rpc_size() -> usize {
    MAX_RPC_SIZE
}

/// Protocol names to be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// The Status protocol name.
    Status,
    /// The Goodbye protocol name.
    Goodbye,
    /// The `Ping` protocol name.
    Ping,
    /// TODO
    DataByHash,

    /// The file answer protocol.
    AnswerFile,
    /// The Chunk sync protocol.
    GetChunks,
}

/// RPC Versions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Version {
    /// Version 1 of RPC
    V1,
}

/// RPC Encodings supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Encoding {
    SSZSnappy,
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            Protocol::Status => "status",
            Protocol::Goodbye => "goodbye",
            Protocol::Ping => "ping",
            Protocol::DataByHash => "data_by_hash",
            Protocol::AnswerFile => "answer_file",
            Protocol::GetChunks => "get_chunks",
        };
        f.write_str(repr)
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            Encoding::SSZSnappy => "ssz_snappy",
        };
        f.write_str(repr)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            Version::V1 => "1",
        };
        f.write_str(repr)
    }
}

#[derive(Debug, Clone)]
pub struct RPCProtocol {
    pub max_rpc_size: usize,
}

impl UpgradeInfo for RPCProtocol {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    /// The list of supported RPC protocols for Lighthouse.
    fn protocol_info(&self) -> Self::InfoIter {
        vec![
            ProtocolId::new(Protocol::Status, Version::V1, Encoding::SSZSnappy),
            ProtocolId::new(Protocol::Goodbye, Version::V1, Encoding::SSZSnappy),
            ProtocolId::new(Protocol::Ping, Version::V1, Encoding::SSZSnappy),
            ProtocolId::new(Protocol::DataByHash, Version::V1, Encoding::SSZSnappy),
            ProtocolId::new(Protocol::AnswerFile, Version::V1, Encoding::SSZSnappy),
            ProtocolId::new(Protocol::GetChunks, Version::V1, Encoding::SSZSnappy),
        ]
    }
}

/// Represents the ssz length bounds for RPC messages.
#[derive(Debug, PartialEq, Eq)]
pub struct RpcLimits {
    pub min: usize,
    pub max: usize,
}

impl RpcLimits {
    pub fn new(min: usize, max: usize) -> Self {
        Self { min, max }
    }

    /// Returns true if the given length is greater than `max_rpc_size` or out of
    /// bounds for the given ssz type, returns false otherwise.
    pub fn is_out_of_bounds(&self, length: usize, max_rpc_size: usize) -> bool {
        length > std::cmp::min(self.max, max_rpc_size) || length < self.min
    }
}

/// Tracks the types in a protocol id.
#[derive(Clone, Debug)]
pub struct ProtocolId {
    /// The RPC message type/name.
    pub message_name: Protocol,

    /// The version of the RPC.
    pub version: Version,

    /// The encoding of the RPC.
    pub encoding: Encoding,

    /// The protocol id that is formed from the above fields.
    protocol_id: String,
}

impl ProtocolId {
    /// Returns min and max size for messages of given protocol id requests.
    pub fn rpc_request_limits(&self) -> RpcLimits {
        match self.message_name {
            Protocol::Status => RpcLimits::new(
                <StatusMessage as Encode>::ssz_fixed_len(),
                <StatusMessage as Encode>::ssz_fixed_len(),
            ),
            Protocol::Goodbye => RpcLimits::new(
                <GoodbyeReason as Encode>::ssz_fixed_len(),
                <GoodbyeReason as Encode>::ssz_fixed_len(),
            ),
            Protocol::Ping => RpcLimits::new(
                <Ping as Encode>::ssz_fixed_len(),
                <Ping as Encode>::ssz_fixed_len(),
            ),
            Protocol::DataByHash => {
                // RpcLimits::new(*DATA_BY_HASH_REQUEST_MIN, *DATA_BY_HASH_REQUEST_MAX)
                // TODO
                RpcLimits::new(1, *DATA_BY_HASH_REQUEST_MAX)
            }
            Protocol::AnswerFile => RpcLimits::new(
                <ShardedFile as Encode>::ssz_fixed_len(),
                <ShardedFile as Encode>::ssz_fixed_len(),
            ),
            Protocol::GetChunks => RpcLimits::new(
                <GetChunksRequest as Encode>::ssz_fixed_len(),
                <GetChunksRequest as Encode>::ssz_fixed_len(),
            ),
        }
    }

    /// Returns min and max size for messages of given protocol id responses.
    pub fn rpc_response_limits(&self) -> RpcLimits {
        match self.message_name {
            Protocol::Status => RpcLimits::new(
                <StatusMessage as Encode>::ssz_fixed_len(),
                <StatusMessage as Encode>::ssz_fixed_len(),
            ),
            Protocol::Goodbye => RpcLimits::new(0, 0), // Goodbye request has no response

            Protocol::Ping => RpcLimits::new(
                <Ping as Encode>::ssz_fixed_len(),
                <Ping as Encode>::ssz_fixed_len(),
            ),

            // TODO: not fixed size?
            Protocol::DataByHash => RpcLimits::new(
                <ZgsData as Encode>::ssz_fixed_len(),
                <ZgsData as Encode>::ssz_fixed_len(),
            ),

            Protocol::AnswerFile => RpcLimits::new(0, 0), // AnswerFile request has no response
            Protocol::GetChunks => RpcLimits::new(*CHUNKS_RESPONSE_MIN, *CHUNKS_RESPONSE_MAX),
        }
    }
}

/// An RPC protocol ID.
impl ProtocolId {
    pub fn new(message_name: Protocol, version: Version, encoding: Encoding) -> Self {
        let protocol_id = format!(
            "{}/{}/{}/{}",
            PROTOCOL_PREFIX, message_name, version, encoding
        );

        ProtocolId {
            message_name,
            version,
            encoding,
            protocol_id,
        }
    }
}

impl ProtocolName for ProtocolId {
    fn protocol_name(&self) -> &[u8] {
        self.protocol_id.as_bytes()
    }
}

/* Inbound upgrade */

// The inbound protocol reads the request, decodes it and returns the stream to the protocol
// handler to respond to once ready.

pub type InboundOutput<TSocket> = (InboundRequest, InboundFramed<TSocket>);
pub type InboundFramed<TSocket> =
    Framed<std::pin::Pin<Box<TimeoutStream<Compat<TSocket>>>>, InboundCodec>;

impl<TSocket> InboundUpgrade<TSocket> for RPCProtocol
where
    TSocket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = InboundOutput<TSocket>;
    type Error = RPCError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, socket: TSocket, protocol: ProtocolId) -> Self::Future {
        async move {
            // convert the socket to tokio compatible socket
            let socket = socket.compat();
            let codec = match protocol.encoding {
                Encoding::SSZSnappy => {
                    let ssz_snappy_codec = BaseInboundCodec::new(SSZSnappyInboundCodec::new(
                        protocol,
                        self.max_rpc_size,
                    ));
                    InboundCodec::SSZSnappy(ssz_snappy_codec)
                }
            };
            let mut timed_socket = TimeoutStream::new(socket);
            timed_socket.set_read_timeout(Some(Duration::from_secs(TTFB_TIMEOUT)));

            let socket = Framed::new(Box::pin(timed_socket), codec);

            match tokio::time::timeout(Duration::from_secs(REQUEST_TIMEOUT), socket.into_future())
                .await
            {
                Err(e) => Err(RPCError::from(e)),
                Ok((Some(Ok(request)), stream)) => Ok((request, stream)),
                Ok((Some(Err(e)), _)) => Err(e),
                Ok((None, _)) => Err(RPCError::IncompleteStream),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundRequest {
    Status(StatusMessage),
    Goodbye(GoodbyeReason),
    Ping(Ping),
    DataByHash(DataByHashRequest),
    AnswerFile(ShardedFile),
    GetChunks(GetChunksRequest),
}

impl UpgradeInfo for InboundRequest {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    // add further protocols as we support more encodings/versions
    fn protocol_info(&self) -> Self::InfoIter {
        self.supported_protocols()
    }
}

/// Implements the encoding per supported protocol for `RPCRequest`.
impl InboundRequest {
    pub fn supported_protocols(&self) -> Vec<ProtocolId> {
        match self {
            // add more protocols when versions/encodings are supported
            InboundRequest::Status(_) => vec![ProtocolId::new(
                Protocol::Status,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            InboundRequest::Goodbye(_) => vec![ProtocolId::new(
                Protocol::Goodbye,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            InboundRequest::Ping(_) => vec![ProtocolId::new(
                Protocol::Ping,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            InboundRequest::DataByHash(_) => vec![ProtocolId::new(
                Protocol::DataByHash,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            InboundRequest::AnswerFile(_) => vec![ProtocolId::new(
                Protocol::AnswerFile,
                Version::V1,
                Encoding::SSZSnappy,
            )],
            InboundRequest::GetChunks(_) => vec![ProtocolId::new(
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
            InboundRequest::Status(_) => 1,
            InboundRequest::Goodbye(_) => 0,
            InboundRequest::DataByHash(req) => req.hashes.len() as u64,
            InboundRequest::Ping(_) => 1,
            InboundRequest::AnswerFile(_) => 0,
            InboundRequest::GetChunks(_) => 1,
        }
    }

    /// Gives the corresponding `Protocol` to this request.
    pub fn protocol(&self) -> Protocol {
        match self {
            InboundRequest::Status(_) => Protocol::Status,
            InboundRequest::Goodbye(_) => Protocol::Goodbye,
            InboundRequest::Ping(_) => Protocol::Ping,
            InboundRequest::DataByHash(_) => Protocol::DataByHash,
            InboundRequest::AnswerFile(_) => Protocol::AnswerFile,
            InboundRequest::GetChunks(_) => Protocol::GetChunks,
        }
    }

    /// Returns the `ResponseTermination` type associated with the request if a stream gets
    /// terminated.
    pub fn stream_termination(&self) -> ResponseTermination {
        match self {
            // this only gets called after `multiple_responses()` returns true. Therefore, only
            // variants that have `multiple_responses()` can have values.
            InboundRequest::DataByHash(_) => ResponseTermination::DataByHash,
            InboundRequest::Status(_) => unreachable!(),
            InboundRequest::Goodbye(_) => unreachable!(),
            InboundRequest::Ping(_) => unreachable!(),
            InboundRequest::AnswerFile(_) => unreachable!(),
            InboundRequest::GetChunks(_) => unreachable!(),
        }
    }
}

/// Error in RPC Encoding/Decoding.
#[derive(Debug, Clone, PartialEq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RPCError {
    /// Error when decoding the raw buffer from ssz.
    // NOTE: in the future a ssz::DecodeError should map to an InvalidData error
    #[strum(serialize = "decode_error")]
    SSZDecodeError(ssz::DecodeError),
    /// IO Error.
    IoError(String),
    /// The peer returned a valid response but the response indicated an error.
    ErrorResponse(RPCResponseErrorCode, String),
    /// Timed out waiting for a response.
    StreamTimeout,
    /// Peer does not support the protocol.
    UnsupportedProtocol,
    /// Stream ended unexpectedly.
    IncompleteStream,
    /// Peer sent invalid data.
    InvalidData(String),
    /// An error occurred due to internal reasons. Ex: timer failure.
    InternalError(&'static str),
    /// Negotiation with this peer timed out.
    NegotiationTimeout,
    /// Handler rejected this request.
    HandlerRejected,
    /// We have intentionally disconnected.
    Disconnected,
}

impl From<ssz::DecodeError> for RPCError {
    #[inline]
    fn from(err: ssz::DecodeError) -> Self {
        RPCError::SSZDecodeError(err)
    }
}
impl From<tokio::time::error::Elapsed> for RPCError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        RPCError::StreamTimeout
    }
}

impl From<io::Error> for RPCError {
    fn from(err: io::Error) -> Self {
        RPCError::IoError(err.to_string())
    }
}

// Error trait is required for `ProtocolsHandler`
impl std::fmt::Display for RPCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            RPCError::SSZDecodeError(ref err) => {
                write!(f, "Error while decoding ssz: {:?}", err)
            }
            RPCError::InvalidData(ref err) => {
                write!(f, "Peer sent unexpected data: {}", err)
            }
            RPCError::IoError(ref err) => write!(f, "IO Error: {}", err),
            RPCError::ErrorResponse(ref code, ref reason) => write!(
                f,
                "RPC response was an error: {} with reason: {}",
                code, reason
            ),
            RPCError::StreamTimeout => write!(f, "Stream Timeout"),
            RPCError::UnsupportedProtocol => {
                write!(f, "Peer does not support the protocol")
            }
            RPCError::IncompleteStream => {
                write!(f, "Stream ended unexpectedly")
            }
            RPCError::InternalError(ref err) => {
                write!(f, "Internal error: {}", err)
            }
            RPCError::NegotiationTimeout => write!(f, "Negotiation timeout"),
            RPCError::HandlerRejected => {
                write!(f, "Handler rejected the request")
            }
            RPCError::Disconnected => write!(f, "Gracefully Disconnected"),
        }
    }
}

impl std::error::Error for RPCError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            // NOTE: this does have a source
            RPCError::SSZDecodeError(_) => None,
            RPCError::IoError(_) => None,
            RPCError::StreamTimeout => None,
            RPCError::UnsupportedProtocol => None,
            RPCError::IncompleteStream => None,
            RPCError::InvalidData(_) => None,
            RPCError::InternalError(_) => None,
            RPCError::ErrorResponse(_, _) => None,
            RPCError::NegotiationTimeout => None,
            RPCError::HandlerRejected => None,
            RPCError::Disconnected => None,
        }
    }
}

impl std::fmt::Display for InboundRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InboundRequest::Status(status) => {
                write!(f, "Status Message: {}", status)
            }
            InboundRequest::Goodbye(reason) => write!(f, "Goodbye: {}", reason),
            InboundRequest::Ping(ping) => write!(f, "Ping: {}", ping.data),
            InboundRequest::DataByHash(req) => {
                write!(f, "Data by hash: {:?}", req)
            }
            InboundRequest::AnswerFile(req) => {
                write!(f, "Answer File: {:?}", req)
            }
            InboundRequest::GetChunks(req) => {
                write!(f, "Get Chunks: {:?}", req)
            }
        }
    }
}

impl RPCError {
    /// Get a `str` representation of the error.
    /// Used for metrics.
    pub fn as_static_str(&self) -> &'static str {
        match self {
            RPCError::ErrorResponse(ref code, ..) => code.into(),
            e => e.into(),
        }
    }
}
