//! Available RPC methods types and ids.

use regex::bytes::Regex;
use ssz_derive::{Decode, Encode};
use ssz_types::{
    typenum::{U1024, U256},
    VariableList,
};
use std::ops::Deref;
use strum::IntoStaticStr;
pub type Hash256 = ethereum_types::H256;
use shared_types::{ChunkArrayWithProof, TxID};

pub use ssz_types::{typenum, typenum::Unsigned, BitList, BitVector, FixedVector};

/// Maximum number of blocks in a single request.
pub type MaxRequestBlocks = U1024;
pub const MAX_REQUEST_BLOCKS: u64 = 1024;

/// Maximum length of error message.
pub type MaxErrorLen = U256;
pub const MAX_ERROR_LEN: u64 = 256;

/// Maximum length of data message.
pub type MaxDataLen = U256;
pub const MAX_DATA_LEN: u64 = 256;

// Maximum length of GetChunksResponse chunk data.
pub const MAX_CHUNKS_LENGTH: usize = 10 * 1024 * 1024; // 10M

#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct ZgsData {
    pub hash: Hash256,
}

/// Wrapper over SSZ List to represent error message in rpc responses.
#[derive(Debug, Clone)]
pub struct ErrorType(pub VariableList<u8, MaxErrorLen>);

impl From<String> for ErrorType {
    fn from(s: String) -> Self {
        Self(VariableList::from(s.as_bytes().to_vec()))
    }
}

impl From<&str> for ErrorType {
    fn from(s: &str) -> Self {
        Self(VariableList::from(s.as_bytes().to_vec()))
    }
}

impl Deref for ErrorType {
    type Target = VariableList<u8, MaxErrorLen>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToString for ErrorType {
    fn to_string(&self) -> String {
        #[allow(clippy::invalid_regex)]
        let re = Regex::new("\\p{C}").expect("Regex is valid");
        String::from_utf8_lossy(&re.replace_all(self.0.deref(), &b""[..])).to_string()
    }
}

/* Request/Response data structures for RPC methods */

/* Requests */

/// The STATUS request/response handshake message.
#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct StatusMessage {
    pub data: u64,
}

/// The PING request/response message.
#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct Ping {
    /// The metadata sequence number.
    pub data: u64,
}

/// The reason given for a `Goodbye` message.
///
/// Note: any unknown `u64::into(n)` will resolve to `Goodbye::Unknown` for any unknown `n`,
/// however `GoodbyeReason::Unknown.into()` will go into `0_u64`. Therefore de-serializing then
/// re-serializing may not return the same bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoodbyeReason {
    /// This node has shutdown.
    ClientShutdown = 1,

    /// Incompatible networks.
    IrrelevantNetwork = 2,

    /// Error/fault in the RPC.
    Fault = 3,

    /// Teku uses this code for not being able to verify a network.
    UnableToVerifyNetwork = 128,

    /// The node has too many connected peers.
    TooManyPeers = 129,

    /// Scored poorly.
    BadScore = 250,

    /// The peer is banned
    Banned = 251,

    /// The IP address the peer is using is banned.
    BannedIP = 252,

    /// Unknown reason.
    Unknown = 0,
}

impl From<u64> for GoodbyeReason {
    fn from(id: u64) -> GoodbyeReason {
        match id {
            1 => GoodbyeReason::ClientShutdown,
            2 => GoodbyeReason::IrrelevantNetwork,
            3 => GoodbyeReason::Fault,
            128 => GoodbyeReason::UnableToVerifyNetwork,
            129 => GoodbyeReason::TooManyPeers,
            250 => GoodbyeReason::BadScore,
            251 => GoodbyeReason::Banned,
            252 => GoodbyeReason::BannedIP,
            _ => GoodbyeReason::Unknown,
        }
    }
}

impl From<GoodbyeReason> for u64 {
    fn from(reason: GoodbyeReason) -> u64 {
        reason as u64
    }
}

impl ssz::Encode for GoodbyeReason {
    fn is_ssz_fixed_len() -> bool {
        <u64 as ssz::Encode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <u64 as ssz::Encode>::ssz_fixed_len()
    }

    fn ssz_bytes_len(&self) -> usize {
        0_u64.ssz_bytes_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        let conv: u64 = self.clone().into();
        conv.ssz_append(buf)
    }
}

impl ssz::Decode for GoodbyeReason {
    fn is_ssz_fixed_len() -> bool {
        <u64 as ssz::Decode>::is_ssz_fixed_len()
    }

    fn ssz_fixed_len() -> usize {
        <u64 as ssz::Decode>::ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, ssz::DecodeError> {
        u64::from_ssz_bytes(bytes).map(|n| n.into())
    }
}

/// Request a number of beacon block bodies from a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataByHashRequest {
    /// The list of beacon block bodies being requested.
    pub hashes: VariableList<Hash256, MaxRequestBlocks>,
}

/// Request a chunk array from a peer.
#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct GetChunksRequest {
    pub tx_id: TxID,
    pub index_start: u64,
    pub index_end: u64,
    pub merkle_tx_seq: u64,
}

/* RPC Handling and Grouping */
// Collection of enums and structs used by the Codecs to encode/decode RPC messages

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RPCResponse {
    /// A HELLO message.
    Status(StatusMessage),

    /// A PONG response to a PING request.
    Pong(Ping),

    /// A response to a get DATA_BY_HASH request.
    DataByHash(Box<ZgsData>),

    /// A response to a GET_CHUNKS request.
    Chunks(ChunkArrayWithProof),
}

/// Indicates which response is being terminated by a stream termination response.
#[derive(Debug, Clone)]
pub enum ResponseTermination {
    /// Data by hash stream termination.
    DataByHash,
}

/// The structured response containing a result/code indicating success or failure
/// and the contents of the response
#[derive(Debug)]
pub enum RPCCodedResponse {
    /// The response is a successful.
    Success(RPCResponse),

    Error(RPCResponseErrorCode, ErrorType),

    /// Received a stream termination indicating which response is being terminated.
    StreamTermination(ResponseTermination),
}

/// The code assigned to an erroneous `RPCResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RPCResponseErrorCode {
    RateLimited,
    InvalidRequest,
    ServerError,
    /// Error spec'd to indicate that a peer does not have blocks on a requested range.
    ResourceUnavailable,
    Unknown,
}

impl RPCCodedResponse {
    /// Used to encode the response in the codec.
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            RPCCodedResponse::Success(_) => Some(0),
            RPCCodedResponse::Error(code, _) => Some(code.as_u8()),
            RPCCodedResponse::StreamTermination(_) => None,
        }
    }

    /// Tells the codec whether to decode as an RPCResponse or an error.
    pub fn is_response(response_code: u8) -> bool {
        matches!(response_code, 0)
    }

    /// Builds an RPCCodedResponse from a response code and an ErrorMessage
    pub fn from_error(response_code: u8, err: ErrorType) -> Self {
        let code = match response_code {
            1 => RPCResponseErrorCode::InvalidRequest,
            2 => RPCResponseErrorCode::ServerError,
            3 => RPCResponseErrorCode::ResourceUnavailable,
            139 => RPCResponseErrorCode::RateLimited,
            _ => RPCResponseErrorCode::Unknown,
        };
        RPCCodedResponse::Error(code, err)
    }

    /// Specifies which response allows for multiple chunks for the stream handler.
    pub fn multiple_responses(&self) -> bool {
        match self {
            RPCCodedResponse::Success(resp) => match resp {
                RPCResponse::Status(_) => false,
                RPCResponse::Pong(_) => false,
                RPCResponse::DataByHash(_) => true,
                RPCResponse::Chunks(_) => false,
            },
            RPCCodedResponse::Error(_, _) => true,
            // Stream terminations are part of responses that have chunks
            RPCCodedResponse::StreamTermination(_) => true,
        }
    }

    /// Returns true if this response always terminates the stream.
    pub fn close_after(&self) -> bool {
        !matches!(self, RPCCodedResponse::Success(_))
    }
}

impl RPCResponseErrorCode {
    fn as_u8(&self) -> u8 {
        match self {
            RPCResponseErrorCode::InvalidRequest => 1,
            RPCResponseErrorCode::ServerError => 2,
            RPCResponseErrorCode::ResourceUnavailable => 3,
            RPCResponseErrorCode::Unknown => 255,
            RPCResponseErrorCode::RateLimited => 139,
        }
    }
}

impl std::fmt::Display for RPCResponseErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            RPCResponseErrorCode::InvalidRequest => "The request was invalid",
            RPCResponseErrorCode::ResourceUnavailable => "Resource unavailable",
            RPCResponseErrorCode::ServerError => "Server error occurred",
            RPCResponseErrorCode::Unknown => "Unknown error occurred",
            RPCResponseErrorCode::RateLimited => "Rate limited",
        };
        f.write_str(repr)
    }
}

impl std::fmt::Display for StatusMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Status Message: Data: {:?}", self.data)
    }
}

impl std::fmt::Display for RPCResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RPCResponse::Status(status) => write!(f, "{}", status),
            RPCResponse::Pong(ping) => write!(f, "Pong: {}", ping.data),
            RPCResponse::DataByHash(data) => {
                write!(f, "DataByHash: Hash: {:?}", data.hash)
            }
            RPCResponse::Chunks(data) => {
                write!(
                    f,
                    "Chunks Response, data length: {}",
                    data.chunks.data.len()
                )
            }
        }
    }
}

impl std::fmt::Display for RPCCodedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RPCCodedResponse::Success(res) => write!(f, "{}", res),
            RPCCodedResponse::Error(code, err) => {
                write!(f, "{}: {}", code, err.to_string())
            }
            RPCCodedResponse::StreamTermination(_) => {
                write!(f, "Stream Termination")
            }
        }
    }
}

impl std::fmt::Display for GoodbyeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GoodbyeReason::ClientShutdown => write!(f, "Client Shutdown"),
            GoodbyeReason::IrrelevantNetwork => write!(f, "Irrelevant Network"),
            GoodbyeReason::Fault => write!(f, "Fault"),
            GoodbyeReason::UnableToVerifyNetwork => {
                write!(f, "Unable to verify network")
            }
            GoodbyeReason::TooManyPeers => write!(f, "Too many peers"),
            GoodbyeReason::BadScore => write!(f, "Bad Score"),
            GoodbyeReason::Banned => write!(f, "Banned"),
            GoodbyeReason::BannedIP => write!(f, "BannedIP"),
            GoodbyeReason::Unknown => write!(f, "Unknown Reason"),
        }
    }
}
