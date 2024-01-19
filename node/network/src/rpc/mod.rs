//! The Ethereum 2.0 Wire Protocol
//!
//! This protocol is a purpose built Ethereum 2.0 libp2p protocol. It's role is to facilitate
//! direct peer-to-peer communication primarily for sending/receiving chain information for
//! syncing.

use futures::future::FutureExt;
use handler::{HandlerEvent, RPCHandler};
use libp2p::core::connection::ConnectionId;
use libp2p::swarm::{
    handler::ConnectionHandler, NetworkBehaviour, NetworkBehaviourAction, NotifyHandler,
    PollParameters, SubstreamProtocol,
};
use libp2p::PeerId;
use rate_limiter::{RPCRateLimiter as RateLimiter, RPCRateLimiterBuilder, RateLimitedErr};
use std::task::{Context, Poll};
use std::time::Duration;

pub(crate) use handler::HandlerErr;
pub(crate) use methods::{Ping, RPCCodedResponse, RPCResponse};
pub(crate) use protocol::{InboundRequest, RPCProtocol};

pub use handler::SubstreamId;
pub use methods::{
    DataByHashRequest, GetChunksRequest, GoodbyeReason, MaxRequestBlocks, RPCResponseErrorCode,
    ResponseTermination, StatusMessage, ZgsData, MAX_REQUEST_BLOCKS,
};
pub(crate) use outbound::OutboundRequest;
pub use protocol::{max_rpc_size, Protocol, RPCError};

pub(crate) mod codec;
mod handler;
pub mod methods;
mod outbound;
mod protocol;
mod rate_limiter;

/// Composite trait for a request id.
pub trait ReqId: Send + 'static + std::fmt::Debug + Copy + Clone {}
impl<T> ReqId for T where T: Send + 'static + std::fmt::Debug + Copy + Clone {}

/// RPC events sent from Lighthouse.
#[derive(Debug)]
pub enum RPCSend<Id> {
    /// A request sent from Lighthouse.
    ///
    /// The `Id` is given by the application making the request. These
    /// go over *outbound* connections.
    Request(Id, OutboundRequest),
    /// A response sent from Lighthouse.
    ///
    /// The `SubstreamId` must correspond to the RPC-given ID of the original request received from the
    /// peer. The second parameter is a single chunk of a response. These go over *inbound*
    /// connections.
    Response(SubstreamId, RPCCodedResponse),
    /// Lighthouse has requested to terminate the connection with a goodbye message.
    Shutdown(Id, GoodbyeReason),
}

/// RPC events received from outside Lighthouse.
#[derive(Debug)]
pub enum RPCReceived<Id> {
    /// A request received from the outside.
    ///
    /// The `SubstreamId` is given by the `RPCHandler` as it identifies this request with the
    /// *inbound* substream over which it is managed.
    Request(SubstreamId, InboundRequest),
    /// A response received from the outside.
    ///
    /// The `Id` corresponds to the application given ID of the original request sent to the
    /// peer. The second parameter is a single chunk of a response. These go over *outbound*
    /// connections.
    Response(Id, RPCResponse),
    /// Marks a request as completed
    EndOfStream(Id, ResponseTermination),
}

impl<Id: std::fmt::Debug> std::fmt::Display for RPCSend<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RPCSend::Request(id, req) => {
                write!(f, "RPC Request(id: {:?}, {})", id, req)
            }
            RPCSend::Response(id, res) => {
                write!(f, "RPC Response(id: {:?}, {})", id, res)
            }
            RPCSend::Shutdown(_id, reason) => {
                write!(f, "Sending Goodbye: {}", reason)
            }
        }
    }
}

/// Messages sent to the user from the RPC protocol.
pub struct RPCMessage<Id> {
    /// The peer that sent the message.
    pub peer_id: PeerId,
    /// Handler managing this message.
    pub conn_id: ConnectionId,
    /// The message that was sent.
    pub event: HandlerEvent<Id>,
}

/// Implements the libp2p `NetworkBehaviour` trait and therefore manages network-level
/// logic.
pub struct RPC<Id: ReqId> {
    /// Rate limiter
    limiter: RateLimiter,
    /// Queue of events to be processed.
    events: Vec<NetworkBehaviourAction<RPCMessage<Id>, RPCHandler<Id>>>,
}

impl<Id: ReqId> RPC<Id> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let limiter = RPCRateLimiterBuilder::new()
            .n_every(Protocol::Ping, 2, Duration::from_secs(10))
            .n_every(Protocol::Status, 5, Duration::from_secs(15))
            .one_every(Protocol::Goodbye, Duration::from_secs(10))
            .n_every(Protocol::DataByHash, 128, Duration::from_secs(10))
            .n_every(Protocol::GetChunks, 4096, Duration::from_secs(10))
            .build()
            .expect("Configuration parameters are valid");
        RPC {
            limiter,
            events: Vec::new(),
        }
    }

    /// Sends an RPC response.
    ///
    /// The peer must be connected for this to succeed.
    pub fn send_response(
        &mut self,
        peer_id: PeerId,
        id: (ConnectionId, SubstreamId),
        event: RPCCodedResponse,
    ) {
        self.events.push(NetworkBehaviourAction::NotifyHandler {
            peer_id,
            handler: NotifyHandler::One(id.0),
            event: RPCSend::Response(id.1, event),
        });
    }

    /// Submits an RPC request.
    ///
    /// The peer must be connected for this to succeed.
    pub fn send_request(&mut self, peer_id: PeerId, request_id: Id, event: OutboundRequest) {
        self.events.push(NetworkBehaviourAction::NotifyHandler {
            peer_id,
            handler: NotifyHandler::Any,
            event: RPCSend::Request(request_id, event),
        });
    }

    /// Lighthouse wishes to disconnect from this peer by sending a Goodbye message. This
    /// gracefully terminates the RPC behaviour with a goodbye message.
    pub fn shutdown(&mut self, peer_id: PeerId, id: Id, reason: GoodbyeReason) {
        self.events.push(NetworkBehaviourAction::NotifyHandler {
            peer_id,
            handler: NotifyHandler::Any,
            event: RPCSend::Shutdown(id, reason),
        });
    }
}

impl<Id> NetworkBehaviour for RPC<Id>
where
    Id: ReqId,
{
    type ConnectionHandler = RPCHandler<Id>;
    type OutEvent = RPCMessage<Id>;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        RPCHandler::new(SubstreamProtocol::new(
            RPCProtocol {
                max_rpc_size: max_rpc_size(),
            },
            (),
        ))
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        conn_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::OutEvent,
    ) {
        if let Ok(RPCReceived::Request(ref id, ref req)) = event {
            // check if the request is conformant to the quota
            match self.limiter.allows(&peer_id, req) {
                Ok(()) => {
                    // send the event to the user
                    self.events
                        .push(NetworkBehaviourAction::GenerateEvent(RPCMessage {
                            peer_id,
                            conn_id,
                            event,
                        }))
                }
                Err(RateLimitedErr::TooLarge) => {
                    // we set the batch sizes, so this is a coding/config err for most protocols
                    let protocol = req.protocol();
                    error!(%protocol, "Request size too large to ever be processed");

                    // send an error code to the peer.
                    // the handler upon receiving the error code will send it back to the behaviour
                    self.send_response(
                        peer_id,
                        (conn_id, *id),
                        RPCCodedResponse::Error(
                            RPCResponseErrorCode::RateLimited,
                            "Rate limited. Request too large".into(),
                        ),
                    );
                }
                Err(RateLimitedErr::TooSoon(wait_time)) => {
                    debug!(
                        request = %req,
                        %peer_id,
                        wait_time_ms = %wait_time.as_millis(),
                        "Request exceeds the rate limit",
                    );

                    // send an error code to the peer.
                    // the handler upon receiving the error code will send it back to the behaviour
                    self.send_response(
                        peer_id,
                        (conn_id, *id),
                        RPCCodedResponse::Error(
                            RPCResponseErrorCode::RateLimited,
                            format!("Wait {:?}", wait_time).into(),
                        ),
                    );
                }
            }
        } else {
            self.events
                .push(NetworkBehaviourAction::GenerateEvent(RPCMessage {
                    peer_id,
                    conn_id,
                    event,
                }));
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        // let the rate limiter prune
        let _ = self.limiter.poll_unpin(cx);
        if !self.events.is_empty() {
            return Poll::Ready(self.events.remove(0));
        }
        Poll::Pending
    }
}
