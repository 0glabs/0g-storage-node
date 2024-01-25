#![allow(clippy::all)]

/// This crate contains the main link for lighthouse to rust-libp2p. It therefore re-exports
/// all required libp2p functionality.
///
/// This crate builds and manages the libp2p services required by the beacon node.
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate tracing;

pub mod behaviour;
mod config;

#[allow(clippy::mutable_key_type)]
// PeerId in hashmaps are no longer permitted by clippy
pub mod discovery;
pub mod metrics;
pub mod nat;
pub mod peer_manager;
pub mod rpc;
mod service;
pub mod types;

pub use config::gossip_max_size;
use std::net::SocketAddr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use shared_types::TxID;
use std::str::FromStr;

/// Wrapper over a libp2p `PeerId` which implements `Serialize` and `Deserialize`
#[derive(Clone, Debug)]
pub struct PeerIdSerialized(libp2p::PeerId);

impl From<PeerIdSerialized> for PeerId {
    fn from(peer_id: PeerIdSerialized) -> Self {
        peer_id.0
    }
}

impl FromStr for PeerIdSerialized {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            PeerId::from_str(s).map_err(|e| format!("Invalid peer id: {}", e))?,
        ))
    }
}

impl Serialize for PeerIdSerialized {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for PeerIdSerialized {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Ok(Self(PeerId::from_str(&s).map_err(|e| {
            de::Error::custom(format!("Failed to deserialise peer id: {:?}", e))
        })?))
    }
}

pub use crate::types::{error, Enr, GossipTopic, NetworkGlobals, PubsubMessage};

pub use prometheus_client;

pub use behaviour::{BehaviourEvent, Gossipsub, PeerRequestId, Request, Response};
pub use config::Config as NetworkConfig;
pub use discovery::{CombinedKeyExt, EnrExt};
pub use discv5;
pub use libp2p;
pub use libp2p::bandwidth::BandwidthSinks;
pub use libp2p::core::identity::{error::SigningError, Keypair, PublicKey};
pub use libp2p::gossipsub::{IdentTopic, MessageAcceptance, MessageId, Topic, TopicHash};
pub use libp2p::{core::ConnectedPoint, PeerId, Swarm};
pub use libp2p::{multiaddr, Multiaddr};
pub use metrics::scrape_discovery_metrics;
pub use peer_manager::{
    peerdb::client::Client,
    peerdb::score::{PeerAction, ReportSource},
    peerdb::PeerDB,
    ConnectionDirection, PeerConnectionStatus, PeerInfo, PeerManager, SyncInfo, SyncStatus,
};
pub use service::{load_private_key, Context, Libp2pEvent, Service, NETWORK_KEY_FILENAME};

/// Application level requests sent to the network.
#[derive(Debug, Clone, Copy)]
pub enum RequestId {
    Router,
    Sync(SyncId),
}

#[derive(Debug, Clone, Copy)]
pub enum SyncId {
    SerialSync { tx_id: TxID },
}

/// Types of messages that the network service can receive.
#[derive(Debug)]
pub enum NetworkMessage {
    /// Send an RPC request to the libp2p service.
    SendRequest {
        peer_id: PeerId,
        request: Request,
        request_id: RequestId,
    },
    /// Send a successful Response to the libp2p service.
    SendResponse {
        peer_id: PeerId,
        response: Response,
        id: PeerRequestId,
    },
    /// Send an error response to an RPC request.
    SendErrorResponse {
        peer_id: PeerId,
        error: rpc::RPCResponseErrorCode,
        reason: String,
        id: PeerRequestId,
    },
    /// Publish a list of messages to the gossipsub protocol.
    Publish { messages: Vec<PubsubMessage> },
    /// Reports a peer to the peer manager for performing an action.
    ReportPeer {
        peer_id: PeerId,
        action: PeerAction,
        source: ReportSource,
        msg: &'static str,
    },
    /// Disconnect an ban a peer, providing a reason.
    GoodbyePeer {
        peer_id: PeerId,
        reason: rpc::GoodbyeReason,
        source: ReportSource,
    },
    /// Start dialing a new peer.
    DialPeer { address: Multiaddr, peer_id: PeerId },
    /// Notify that new file stored in db.
    AnnounceLocalFile { tx_id: TxID },
    /// Called if a known external TCP socket address has been updated.
    UPnPMappingEstablished {
        /// The external TCP address has been updated.
        tcp_socket: Option<SocketAddr>,
        /// The external UDP address has been updated.
        udp_socket: Option<SocketAddr>,
    },
}
