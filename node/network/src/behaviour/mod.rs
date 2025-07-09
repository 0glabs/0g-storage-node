use crate::behaviour::gossipsub_scoring_parameters::lighthouse_gossip_thresholds;
use crate::config::gossipsub_config;
use crate::discovery::{Discovery, DiscoveryEvent, FIND_NODE_QUERY_CLOSEST_PEERS};
use crate::peer_manager::{
    config::Config as PeerManagerCfg, peerdb::score::PeerAction, peerdb::score::ReportSource,
    ConnectionDirection, PeerManager, PeerManagerEvent,
};
use crate::rpc::methods::DataByHashRequest;
use crate::rpc::methods::GetChunksRequest;
use crate::rpc::*;
use crate::service::Context as ServiceContext;
use crate::types::{GossipEncoding, GossipKind, GossipTopic, SnappyTransform};
use crate::{error, metrics, Enr, NetworkGlobals, PubsubMessage, TopicHash};
use futures::stream::StreamExt;
use libp2p::gossipsub::error::PublishError;
use libp2p::gossipsub::TopicScoreParams;
use libp2p::{
    core::{
        connection::ConnectionId, identity::Keypair, multiaddr::Protocol as MProtocol, Multiaddr,
    },
    gossipsub::{
        subscription_filter::{AllowAllSubscriptionFilter, MaxCountSubscriptionFilter},
        Gossipsub as BaseGossipsub, GossipsubEvent, IdentTopic as Topic, MessageAcceptance,
        MessageAuthenticity, MessageId,
    },
    identify::{Identify, IdentifyConfig, IdentifyEvent},
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        AddressScore, NetworkBehaviour, NetworkBehaviourAction as NBAction,
        NetworkBehaviourEventProcess, PollParameters,
    },
    NetworkBehaviour, PeerId,
};
use shared_types::{ChunkArrayWithProof, ShardedFile};
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
};

use self::gossip_cache::GossipCache;

mod gossip_cache;
pub mod gossipsub_scoring_parameters;

/// The number of peers we target per subnet for discovery queries.
pub const TARGET_SUBNET_PEERS: usize = 6;

const MAX_IDENTIFY_ADDRESSES: usize = 10;

/// Identifier of requests sent by a peer.
pub type PeerRequestId = (ConnectionId, SubstreamId);

pub type SubscriptionFilter = MaxCountSubscriptionFilter<AllowAllSubscriptionFilter>;
pub type Gossipsub = BaseGossipsub<SnappyTransform, SubscriptionFilter>;

/// Identifier of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestId<AppReqId> {
    Application(AppReqId),
    Behaviour,
}

/// The types of events than can be obtained from polling the behaviour.
#[derive(Debug)]
pub enum BehaviourEvent<AppReqId: ReqId> {
    /// We have successfully dialed and connected to a peer.
    PeerConnectedOutgoing(PeerId),
    /// A peer has successfully dialed and connected to us.
    PeerConnectedIncoming(PeerId),
    /// A peer has disconnected.
    PeerDisconnected(PeerId),
    /// The peer needs to be banned.
    PeerBanned(PeerId),
    /// The peer has been unbanned.
    PeerUnbanned(PeerId),
    /// An RPC Request that was sent failed.
    RPCFailed {
        /// The id of the failed request.
        id: AppReqId,
        /// The peer to which this request was sent.
        peer_id: PeerId,
    },
    RequestReceived {
        /// The peer that sent the request.
        peer_id: PeerId,
        /// Identifier of the request. All responses to this request must use this id.
        id: PeerRequestId,
        /// Request the peer sent.
        request: Request,
    },
    ResponseReceived {
        /// Peer that sent the response.
        peer_id: PeerId,
        /// Id of the request to which the peer is responding.
        id: AppReqId,
        /// Response the peer sent.
        response: Response,
    },
    PubsubMessage {
        /// The gossipsub message id. Used when propagating blocks after validation.
        id: MessageId,
        /// The peer from which we received this message, not the peer that published it.
        propagation_source: PeerId,
        /// The peer who published and signed this message.
        source: PeerId,
        /// The topic that this message was sent on.
        topic: TopicHash,
        /// The message itself.
        message: PubsubMessage,
    },
    /// Inform the network to send a Status to this peer.
    StatusPeer(PeerId),
}

/// Internal type to pass messages from sub-behaviours to the poll of the global behaviour to be
/// specified as an NBAction.
enum InternalBehaviourMessage {
    /// Dial a Peer.
    DialPeer(PeerId),
    /// The socket has been updated.
    SocketUpdated(Multiaddr),
}

/// Builds the network behaviour that manages the core protocols of eth2.
/// This core behaviour is managed by `Behaviour` which adds peer management to all core
/// behaviours.
#[derive(NetworkBehaviour)]
#[behaviour(
    out_event = "BehaviourEvent<AppReqId>",
    poll_method = "poll",
    event_process = true
)]
pub struct Behaviour<AppReqId: ReqId> {
    /* Sub-Behaviours */
    /// The routing pub-sub mechanism for eth2.
    gossipsub: Gossipsub,
    /// The Eth2 RPC specified in the wire-0 protocol.
    eth2_rpc: RPC<RequestId<AppReqId>>,
    /// Discv5 Discovery protocol.
    discovery: Discovery,
    /// Keep regular connection to peers and disconnect if absent.
    // NOTE: The id protocol is used for initial interop. This will be removed by mainnet.
    /// Provides IP addresses and peer information.
    identify: Identify,
    /// The peer manager that keeps track of peer's reputation and status.
    peer_manager: PeerManager,

    /* Auxiliary Fields */
    /// The output events generated by this behaviour to be consumed in the swarm poll.
    #[behaviour(ignore)]
    events: VecDeque<BehaviourEvent<AppReqId>>,
    /// Internal behaviour events, the NBAction type is composed of sub-behaviours, so we use a
    /// custom type here to avoid having to specify the concrete type.
    #[behaviour(ignore)]
    internal_events: VecDeque<InternalBehaviourMessage>,
    /// A collections of variables accessible outside the network service.
    #[behaviour(ignore)]
    network_globals: Arc<NetworkGlobals>,
    /// The waker for the current task. This is used to wake the task when events are added to the
    /// queue.
    #[behaviour(ignore)]
    waker: Option<std::task::Waker>,
    /// The interval for updating gossipsub scores
    #[behaviour(ignore)]
    update_gossipsub_scores: tokio::time::Interval,
    #[behaviour(ignore)]
    gossip_cache: GossipCache,
}

/// Implements the combined behaviour for the libp2p service.
impl<AppReqId: ReqId> Behaviour<AppReqId> {
    pub async fn new(
        local_key: &Keypair,
        ctx: ServiceContext<'_>,
        network_globals: Arc<NetworkGlobals>,
    ) -> error::Result<Self> {
        let mut config = ctx.config.clone();

        // Set up the Identify Behaviour
        let identify_config = if config.private {
            IdentifyConfig::new(
                "".into(),
                local_key.public(), // Still send legitimate public key
            )
            .with_cache_size(0)
        } else {
            IdentifyConfig::new("eth2/1.0.0".into(), local_key.public())
                .with_agent_version(zgs_version::version_with_platform())
                .with_cache_size(0)
        };

        // Build and start the discovery sub-behaviour
        let mut discovery = Discovery::new(local_key, &config, network_globals.clone()).await?;
        // start searching for peers
        discovery.discover_peers(FIND_NODE_QUERY_CLOSEST_PEERS);

        let filter = MaxCountSubscriptionFilter {
            filter: AllowAllSubscriptionFilter {},
            max_subscribed_topics: 200,
            max_subscriptions_per_request: 150, // 148 in theory = (64 attestation + 4 sync committee + 6 core topics) * 2
        };

        config.gs_config = gossipsub_config(config.network_load);

        // If metrics are enabled for gossipsub build the configuration
        let snappy_transform = SnappyTransform::new(config.gs_config.max_transmit_size());
        let mut gossipsub = Gossipsub::new_with_subscription_filter_and_transform(
            MessageAuthenticity::Signed(local_key.clone()),
            config.gs_config.clone(),
            None, // gossipsub_metrics
            filter,
            snappy_transform,
        )
        .map_err(|e| format!("Could not construct gossipsub: {:?}", e))?;

        // Construct a set of gossipsub peer scoring parameters
        // We don't know the number of active validators and the current slot yet
        let thresholds = lighthouse_gossip_thresholds();

        // let score_settings = PeerScoreSettings::new(ctx.chain_spec);

        // // Prepare scoring parameters
        // let params = score_settings.get_peer_score_params(
        //     &thresholds,
        // )?;

        // trace!(behaviour_log, "Using peer score params"; "params" => ?params);

        let mut params = libp2p::gossipsub::PeerScoreParams::default();
        let get_hash = |kind: GossipKind| -> TopicHash {
            let topic: Topic = GossipTopic::new(kind, GossipEncoding::default()).into();
            topic.hash()
        };
        params
            .topics
            .insert(get_hash(GossipKind::NewFile), TopicScoreParams::default());
        params
            .topics
            .insert(get_hash(GossipKind::AskFile), TopicScoreParams::default());
        params
            .topics
            .insert(get_hash(GossipKind::FindFile), TopicScoreParams::default());
        params.topics.insert(
            get_hash(GossipKind::FindChunks),
            TopicScoreParams::default(),
        );
        params.topics.insert(
            get_hash(GossipKind::AnnounceFile),
            TopicScoreParams::default(),
        );
        params.topics.insert(
            get_hash(GossipKind::AnnounceShardConfig),
            TopicScoreParams::default(),
        );
        params.topics.insert(
            get_hash(GossipKind::AnnounceChunks),
            TopicScoreParams::default(),
        );

        // Set up a scoring update interval
        let update_gossipsub_scores = tokio::time::interval(params.decay_interval);

        gossipsub
            .with_peer_score(params, thresholds)
            .expect("Valid score params and thresholds");

        let peer_manager_cfg = PeerManagerCfg {
            discovery_enabled: !config.disable_discovery,
            metrics_enabled: config.metrics_enabled,
            target_peer_count: config.target_peers,
            ..config.peer_manager
        };

        // let slot_duration = std::time::Duration::from_secs(12);
        // let slot_duration = std::time::Duration::from_secs(ctx.chain_spec.seconds_per_slot);

        let gossip_cache = GossipCache::default();

        Ok(Behaviour {
            // Sub-behaviours
            gossipsub,
            eth2_rpc: RPC::new(),
            discovery,
            identify: Identify::new(identify_config),
            // Auxiliary fields
            peer_manager: PeerManager::new(peer_manager_cfg, network_globals.clone()).await?,
            events: VecDeque::new(),
            internal_events: VecDeque::new(),
            network_globals,
            waker: None,
            gossip_cache,
            update_gossipsub_scores,
        })
    }

    /* Public Accessible Functions to interact with the behaviour */

    /// Get a mutable reference to the underlying discovery sub-behaviour.
    pub fn discovery_mut(&mut self) -> &mut Discovery {
        &mut self.discovery
    }

    /// Get a mutable reference to the peer manager.
    pub fn peer_manager_mut(&mut self) -> &mut PeerManager {
        &mut self.peer_manager
    }

    /// Returns the local ENR of the node.
    pub fn local_enr(&self) -> Enr {
        self.network_globals.local_enr()
    }

    /// Obtain a reference to the gossipsub protocol.
    pub fn gs(&self) -> &Gossipsub {
        &self.gossipsub
    }

    /* Pubsub behaviour functions */

    /// Subscribes to a gossipsub topic kind, letting the network service determine the
    /// encoding and fork version.
    pub fn subscribe_kind(&mut self, kind: GossipKind) -> bool {
        let gossip_topic = GossipTopic::new(kind, GossipEncoding::default());

        self.subscribe(gossip_topic)
    }

    /// Unsubscribes from a gossipsub topic kind, letting the network service determine the
    /// encoding and fork version.
    pub fn unsubscribe_kind(&mut self, kind: GossipKind) -> bool {
        let gossip_topic = GossipTopic::new(kind, GossipEncoding::default());
        self.unsubscribe(gossip_topic)
    }

    /// Subscribes to a gossipsub topic.
    ///
    /// Returns `true` if the subscription was successful and `false` otherwise.
    pub fn subscribe(&mut self, topic: GossipTopic) -> bool {
        // update the network globals
        self.network_globals
            .gossipsub_subscriptions
            .write()
            .insert(topic.clone());

        let topic: Topic = topic.into();

        match self.gossipsub.subscribe(&topic) {
            Err(e) => {
                warn!(%topic, error = ?e, "Failed to subscribe to topic");
                false
            }
            Ok(_) => {
                debug!(%topic, "Subscribed to topic");
                true
            }
        }
    }

    /// Unsubscribe from a gossipsub topic.
    pub fn unsubscribe(&mut self, topic: GossipTopic) -> bool {
        // update the network globals
        self.network_globals
            .gossipsub_subscriptions
            .write()
            .remove(&topic);

        // unsubscribe from the topic
        let libp2p_topic: Topic = topic.clone().into();

        match self.gossipsub.unsubscribe(&libp2p_topic) {
            Err(_) => {
                warn!(topic = %libp2p_topic, "Failed to unsubscribe from topic");
                false
            }
            Ok(v) => {
                // Inform the network
                debug!(%topic, "Unsubscribed to topic");
                v
            }
        }
    }

    /// Publishes a list of messages on the pubsub (gossipsub) behaviour, choosing the encoding.
    pub fn publish(&mut self, messages: Vec<PubsubMessage>) {
        for message in messages {
            for topic in message.topics(GossipEncoding::default()) {
                let message_data = message.encode(GossipEncoding::default());
                if let Err(e) = self
                    .gossipsub
                    .publish(topic.clone().into(), message_data.clone())
                {
                    trace!(error = ?e, topic = ?topic.kind(), "Failed to publish message");

                    // add to metrics
                    if let Some(v) = metrics::get_int_gauge(
                        &metrics::FAILED_PUBLISHES_PER_MAIN_TOPIC,
                        &[&format!("{:?}", topic.kind())],
                    ) {
                        v.inc()
                    };

                    if let PublishError::InsufficientPeers = e {
                        self.gossip_cache.insert(topic, message_data);
                    }
                }
            }
        }
    }

    /// Informs the gossipsub about the result of a message validation.
    /// If the message is valid it will get propagated by gossipsub.
    pub fn report_message_validation_result(
        &mut self,
        propagation_source: &PeerId,
        message_id: MessageId,
        validation_result: MessageAcceptance,
    ) {
        if let Some(result) = match validation_result {
            MessageAcceptance::Accept => None,
            MessageAcceptance::Ignore => Some("ignore"),
            MessageAcceptance::Reject => Some("reject"),
        } {
            if let Some(client) = self
                .network_globals
                .peers
                .read()
                .peer_info(propagation_source)
                .map(|info| info.client().kind.as_ref())
            {
                metrics::inc_counter_vec(
                    &metrics::GOSSIP_UNACCEPTED_MESSAGES_PER_CLIENT,
                    &[client, result],
                )
            }
        }

        if let Err(e) = self.gossipsub.report_message_validation_result(
            &message_id,
            propagation_source,
            validation_result,
        ) {
            warn!(
                %message_id,
                peer_id = %propagation_source,
                error = ?e,
                "Failed to report message validation",
            );
        }
    }

    /* Eth2 RPC behaviour functions */

    /// Send a request to a peer over RPC.
    pub fn send_request(&mut self, peer_id: PeerId, request_id: AppReqId, request: Request) {
        self.eth2_rpc
            .send_request(peer_id, RequestId::Application(request_id), request.into())
    }

    /// Send a successful response to a peer over RPC.
    pub fn send_successful_response(
        &mut self,
        peer_id: PeerId,
        id: PeerRequestId,
        response: Response,
    ) {
        self.eth2_rpc.send_response(peer_id, id, response.into())
    }

    /// Inform the peer that their request produced an error.
    pub fn send_error_reponse(
        &mut self,
        peer_id: PeerId,
        id: PeerRequestId,
        error: RPCResponseErrorCode,
        reason: String,
    ) {
        self.eth2_rpc
            .send_response(peer_id, id, RPCCodedResponse::Error(error, reason.into()))
    }

    /* Peer management functions */

    /// Disconnects from a peer providing a reason.
    ///
    /// This will send a goodbye, disconnect and then ban the peer.
    /// This is fatal for a peer, and should be used in unrecoverable circumstances.
    pub fn goodbye_peer(&mut self, peer_id: &PeerId, reason: GoodbyeReason, source: ReportSource) {
        self.peer_manager.goodbye_peer(peer_id, reason, source);
    }

    /// Returns an iterator over all enr entries in the DHT.
    pub fn enr_entries(&mut self) -> Vec<Enr> {
        self.discovery.table_entries_enr()
    }

    /// Add an ENR to the routing table of the discovery mechanism.
    pub fn add_enr(&mut self, enr: Enr) {
        self.discovery.add_enr(enr);
    }

    /* Private internal functions */

    /// Sends a Ping request to the peer.
    fn ping(&mut self, peer_id: PeerId) {
        let ping = crate::rpc::Ping { data: 1 };
        trace!(%peer_id, "Sending Ping");
        let id = RequestId::Behaviour;
        self.eth2_rpc
            .send_request(peer_id, id, OutboundRequest::Ping(ping));
    }

    /// Sends a Pong response to the peer.
    fn pong(&mut self, id: PeerRequestId, peer_id: PeerId) {
        let ping = crate::rpc::Ping { data: 1 };
        trace!(request_id = ?id.1, %peer_id, "Sending Pong");
        let event = RPCCodedResponse::Success(RPCResponse::Pong(ping));
        self.eth2_rpc.send_response(peer_id, id, event);
    }

    /// Returns a reference to the peer manager to allow the swarm to notify the manager of peer
    /// status
    pub fn peer_manager(&mut self) -> &mut PeerManager {
        &mut self.peer_manager
    }

    // RPC Propagation methods
    /// Queues the response to be sent upwards as long at it was requested outside the Behaviour.
    fn propagate_response(&mut self, id: RequestId<AppReqId>, peer_id: PeerId, response: Response) {
        match id {
            RequestId::Application(id) => self.add_event(BehaviourEvent::ResponseReceived {
                peer_id,
                id,
                response,
            }),
            RequestId::Behaviour => {}
        }
    }

    /// Convenience function to propagate a request.
    fn propagate_request(&mut self, id: PeerRequestId, peer_id: PeerId, request: Request) {
        // Increment metrics
        match &request {
            Request::Status(_) => {
                metrics::inc_counter_vec(&metrics::TOTAL_RPC_REQUESTS, &["status"])
            }
            Request::DataByHash { .. } => {
                metrics::inc_counter_vec(&metrics::TOTAL_RPC_REQUESTS, &["data_by_hash"])
            }
            Request::AnswerFile { .. } => {
                metrics::inc_counter_vec(&metrics::TOTAL_RPC_REQUESTS, &["answer_file"])
            }
            Request::GetChunks { .. } => {
                metrics::inc_counter_vec(&metrics::TOTAL_RPC_REQUESTS, &["get_chunks"])
            }
        }
        self.add_event(BehaviourEvent::RequestReceived {
            peer_id,
            id,
            request,
        });
    }

    /// Adds an event to the queue waking the current task to process it.
    fn add_event(&mut self, event: BehaviourEvent<AppReqId>) {
        self.events.push_back(event);
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

/* Behaviour Event Process Implementations
 *
 * These implementations dictate how to process each event that is emitted from each
 * sub-behaviour.
 */

// Gossipsub
impl<AppReqId> NetworkBehaviourEventProcess<GossipsubEvent> for Behaviour<AppReqId>
where
    AppReqId: ReqId,
{
    fn inject_event(&mut self, event: GossipsubEvent) {
        match event {
            GossipsubEvent::Message {
                propagation_source,
                message_id: id,
                message: gs_msg,
            } => {
                // Note: We are keeping track here of the peer that sent us the message, not the
                // peer that originally published the message.
                match PubsubMessage::decode(&gs_msg.topic, &gs_msg.data) {
                    Err(e) => {
                        debug!(topic = ?gs_msg.topic, %propagation_source, error = ?e, "Could not decode gossipsub message");
                        //reject the message
                        if let Err(e) = self.gossipsub.report_message_validation_result(
                            &id,
                            &propagation_source,
                            MessageAcceptance::Reject,
                        ) {
                            warn!(message_id = %id, peer_id = %propagation_source, error = ?e, "Failed to report message validation");
                        }

                        self.peer_manager.report_peer(
                            &propagation_source,
                            PeerAction::Fatal,
                            ReportSource::Gossipsub,
                            None,
                            "gossipsub message decode error",
                        );

                        if let Some(source) = &gs_msg.source {
                            self.peer_manager.report_peer(
                                source,
                                PeerAction::Fatal,
                                ReportSource::Gossipsub,
                                None,
                                "gossipsub message decode error",
                            );
                        }
                    }
                    Ok(msg) => {
                        // Notify the network
                        self.add_event(BehaviourEvent::PubsubMessage {
                            id,
                            propagation_source,
                            source: gs_msg.source.expect("message is signed"), // TODO: is this guaranteed?
                            topic: gs_msg.topic,
                            message: msg,
                        });
                    }
                }
            }
            GossipsubEvent::Subscribed { peer_id: _, topic } => {
                if let Ok(topic) = GossipTopic::decode(topic.as_str()) {
                    // if let Some(subnet_id) = topic.subnet_id() {
                    //     self.network_globals
                    //         .peers
                    //         .write()
                    //         .add_subscription(&peer_id, subnet_id);
                    // }
                    // Try to send the cached messages for this topic
                    if let Some(msgs) = self.gossip_cache.retrieve(&topic) {
                        for data in msgs {
                            let topic_str: &str = topic.kind().as_ref();
                            match self.gossipsub.publish(topic.clone().into(), data) {
                                Ok(_) => {
                                    warn!(topic = topic_str, "Gossip message published on retry");
                                    if let Some(v) = metrics::get_int_counter(
                                        &metrics::GOSSIP_LATE_PUBLISH_PER_TOPIC_KIND,
                                        &[topic_str],
                                    ) {
                                        v.inc()
                                    };
                                }
                                Err(e) => {
                                    warn!(topic = topic_str, error = %e, "Gossip message publish failed on retry");
                                    if let Some(v) = metrics::get_int_counter(
                                        &metrics::GOSSIP_FAILED_LATE_PUBLISH_PER_TOPIC_KIND,
                                        &[topic_str],
                                    ) {
                                        v.inc()
                                    };
                                }
                            }
                        }
                    }
                }
            }
            GossipsubEvent::Unsubscribed {
                peer_id: _,
                topic: _,
            } => {
                // if let Some(subnet_id) = subnet_from_topic_hash(&topic) {
                //     self.network_globals
                //         .peers
                //         .write()
                //         .remove_subscription(&peer_id, &subnet_id);
                // }
            }
            GossipsubEvent::GossipsubNotSupported { peer_id } => {
                debug!(%peer_id, "Peer does not support gossipsub");
                self.peer_manager.report_peer(
                    &peer_id,
                    PeerAction::LowToleranceError,
                    ReportSource::Gossipsub,
                    Some(GoodbyeReason::Unknown),
                    "does_not_support_gossipsub",
                );
            }
        }
    }
}

// RPC
impl<AppReqId> NetworkBehaviourEventProcess<RPCMessage<RequestId<AppReqId>>> for Behaviour<AppReqId>
where
    AppReqId: ReqId,
{
    fn inject_event(&mut self, event: RPCMessage<RequestId<AppReqId>>) {
        let peer_id = event.peer_id;

        if !self.peer_manager.is_connected(&peer_id) {
            debug!(
                peer = %peer_id,
                "Ignoring rpc message of disconnecting peer"
            );
            return;
        }

        let handler_id = event.conn_id;
        // The METADATA and PING RPC responses are handled within the behaviour and not propagated
        match event.event {
            Err(handler_err) => {
                match handler_err {
                    HandlerErr::Inbound {
                        id: _,
                        proto,
                        error,
                    } => {
                        if matches!(error, RPCError::HandlerRejected) {
                            // this peer's request got canceled
                        }
                        // Inform the peer manager of the error.
                        // An inbound error here means we sent an error to the peer, or the stream
                        // timed out.
                        self.peer_manager.handle_rpc_error(
                            &peer_id,
                            proto,
                            &error,
                            ConnectionDirection::Incoming,
                        );
                    }
                    HandlerErr::Outbound { id, proto, error } => {
                        // Inform the peer manager that a request we sent to the peer failed
                        self.peer_manager.handle_rpc_error(
                            &peer_id,
                            proto,
                            &error,
                            ConnectionDirection::Outgoing,
                        );
                        // inform failures of requests coming outside the behaviour
                        if let RequestId::Application(id) = id {
                            self.add_event(BehaviourEvent::RPCFailed { peer_id, id });
                        }
                    }
                }
            }
            Ok(RPCReceived::Request(id, request)) => {
                let peer_request_id = (handler_id, id);
                match request {
                    /* Behaviour managed protocols: Ping and Metadata */
                    InboundRequest::Ping(ping) => {
                        // inform the peer manager and send the response
                        self.peer_manager.ping_request(&peer_id, ping.data);
                        // send a ping response
                        self.pong(peer_request_id, peer_id);
                    }
                    InboundRequest::Goodbye(reason) => {
                        // queue for disconnection without a goodbye message
                        debug!(
                            %peer_id,
                            %reason,
                            client = %self.network_globals.client(&peer_id),
                            "Peer sent Goodbye"
                        );
                        // NOTE: We currently do not inform the application that we are
                        // disconnecting here. The RPC handler will automatically
                        // disconnect for us.
                        // The actual disconnection event will be relayed to the application.
                    }
                    /* Protocols propagated to the Network */
                    InboundRequest::Status(msg) => {
                        // inform the peer manager that we have received a status from a peer
                        self.peer_manager.peer_statusd(&peer_id);
                        // propagate the STATUS message upwards
                        self.propagate_request(peer_request_id, peer_id, Request::Status(msg))
                    }
                    InboundRequest::DataByHash(req) => {
                        self.propagate_request(peer_request_id, peer_id, Request::DataByHash(req))
                    }
                    InboundRequest::AnswerFile(req) => {
                        self.propagate_request(peer_request_id, peer_id, Request::AnswerFile(req))
                    }
                    InboundRequest::GetChunks(req) => {
                        self.propagate_request(peer_request_id, peer_id, Request::GetChunks(req))
                    }
                }
            }
            Ok(RPCReceived::Response(id, resp)) => {
                match resp {
                    /* Behaviour managed protocols */
                    RPCResponse::Pong(ping) => self.peer_manager.pong_response(&peer_id, ping.data),
                    /* Network propagated protocols */
                    RPCResponse::Status(msg) => {
                        // inform the peer manager that we have received a status from a peer
                        self.peer_manager.peer_statusd(&peer_id);
                        // propagate the STATUS message upwards
                        self.propagate_response(id, peer_id, Response::Status(msg));
                    }
                    RPCResponse::DataByHash(resp) => {
                        self.propagate_response(id, peer_id, Response::DataByHash(Some(resp)))
                    }
                    RPCResponse::Chunks(resp) => {
                        self.propagate_response(id, peer_id, Response::Chunks(resp))
                    }
                }
            }
            Ok(RPCReceived::EndOfStream(id, termination)) => {
                let response = match termination {
                    ResponseTermination::DataByHash => Response::DataByHash(None),
                };
                self.propagate_response(id, peer_id, response);
            }
        }
    }
}

// Discovery
impl<AppReqId> NetworkBehaviourEventProcess<DiscoveryEvent> for Behaviour<AppReqId>
where
    AppReqId: ReqId,
{
    fn inject_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::SocketUpdated(socket_addr) => {
                // A new UDP socket has been detected.
                // Build a multiaddr to report to libp2p
                let mut multiaddr = Multiaddr::from(socket_addr.ip());
                // NOTE: This doesn't actually track the external TCP port. More sophisticated NAT handling
                // should handle this.
                multiaddr.push(MProtocol::Tcp(self.network_globals.listen_port_tcp()));
                self.internal_events
                    .push_back(InternalBehaviourMessage::SocketUpdated(multiaddr));
            }
            DiscoveryEvent::QueryResult(results) => {
                let to_dial_peers = self.peer_manager.peers_discovered(results);
                for peer_id in to_dial_peers {
                    debug!(%peer_id, "Dialing discovered peer");
                    // For any dial event, inform the peer manager
                    let enr = self.discovery_mut().enr_of_peer(&peer_id);
                    self.peer_manager.inject_dialing(&peer_id, enr);
                    self.internal_events
                        .push_back(InternalBehaviourMessage::DialPeer(peer_id));
                }
            }
        }
    }
}

// Identify
impl<AppReqId> NetworkBehaviourEventProcess<IdentifyEvent> for Behaviour<AppReqId>
where
    AppReqId: ReqId,
{
    fn inject_event(&mut self, event: IdentifyEvent) {
        match event {
            IdentifyEvent::Received { peer_id, mut info } => {
                if info.listen_addrs.len() > MAX_IDENTIFY_ADDRESSES {
                    debug!("More than 10 addresses have been identified, truncating");
                    info.listen_addrs.truncate(MAX_IDENTIFY_ADDRESSES);
                }
                // send peer info to the peer manager.
                self.peer_manager.identify(&peer_id, &info);
            }
            IdentifyEvent::Sent { .. } => {}
            IdentifyEvent::Error { .. } => {}
            IdentifyEvent::Pushed { .. } => {}
        }
    }
}

type BehaviourHandler<AppReqId> = <Behaviour<AppReqId> as NetworkBehaviour>::ConnectionHandler;

impl<AppReqId> Behaviour<AppReqId>
where
    AppReqId: ReqId,
{
    /// Consumes the events list and drives the Lighthouse global NetworkBehaviour.
    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NBAction<BehaviourEvent<AppReqId>, BehaviourHandler<AppReqId>>> {
        if let Some(waker) = &self.waker {
            if waker.will_wake(cx.waker()) {
                self.waker = Some(cx.waker().clone());
            }
        } else {
            self.waker = Some(cx.waker().clone());
        }

        // Handle internal events first
        if let Some(event) = self.internal_events.pop_front() {
            match event {
                InternalBehaviourMessage::DialPeer(peer_id) => {
                    // Submit the event
                    let handler = self.new_handler();
                    return Poll::Ready(NBAction::Dial {
                        opts: DialOpts::peer_id(peer_id)
                            .condition(PeerCondition::Disconnected)
                            .build(),
                        handler,
                    });
                }
                InternalBehaviourMessage::SocketUpdated(address) => {
                    return Poll::Ready(NBAction::ReportObservedAddr {
                        address,
                        score: AddressScore::Finite(1),
                    });
                }
            }
        }

        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(NBAction::GenerateEvent(event));
        }

        // perform gossipsub score updates when necessary
        while self.update_gossipsub_scores.poll_tick(cx).is_ready() {
            self.peer_manager.update_gossipsub_scores(&self.gossipsub);
        }

        // poll the gossipsub cache to clear expired messages
        while let Poll::Ready(Some(result)) = self.gossip_cache.poll_next_unpin(cx) {
            match result {
                Err(e) => warn!(error = ?e, "Gossip cache error"),
                Ok(expired_topic) => {
                    if let Some(v) = metrics::get_int_counter(
                        &metrics::GOSSIP_EXPIRED_LATE_PUBLISH_PER_TOPIC_KIND,
                        &[expired_topic.kind().as_ref()],
                    ) {
                        v.inc()
                    };
                }
            }
        }

        Poll::Pending
    }
}

impl<AppReqId: ReqId> NetworkBehaviourEventProcess<PeerManagerEvent> for Behaviour<AppReqId> {
    fn inject_event(&mut self, event: PeerManagerEvent) {
        match event {
            PeerManagerEvent::PeerConnectedIncoming(peer_id) => {
                self.add_event(BehaviourEvent::PeerConnectedIncoming(peer_id));
            }
            PeerManagerEvent::PeerConnectedOutgoing(peer_id) => {
                self.add_event(BehaviourEvent::PeerConnectedOutgoing(peer_id));
            }
            PeerManagerEvent::PeerDisconnected(peer_id) => {
                self.add_event(BehaviourEvent::PeerDisconnected(peer_id));
            }
            PeerManagerEvent::Banned(peer_id, associated_ips) => {
                self.discovery.ban_peer(&peer_id, associated_ips);
                self.add_event(BehaviourEvent::PeerBanned(peer_id));
            }
            PeerManagerEvent::UnBanned(peer_id, associated_ips) => {
                self.discovery.unban_peer(&peer_id, associated_ips);
                self.add_event(BehaviourEvent::PeerUnbanned(peer_id));
            }
            PeerManagerEvent::Status(peer_id) => {
                // it's time to status. We don't keep a beacon chain reference here, so we inform
                // the network to send a status to this peer
                self.add_event(BehaviourEvent::StatusPeer(peer_id));
            }
            PeerManagerEvent::DiscoverPeers(peers_to_find) => {
                // Peer manager has requested a discovery query for more peers.
                self.discovery.discover_peers(peers_to_find);
            }
            PeerManagerEvent::Ping(peer_id) => {
                // send a ping request to this peer
                self.ping(peer_id);
            }
            PeerManagerEvent::DisconnectPeer(peer_id, reason) => {
                debug!(%peer_id, %reason, "Peer Manager disconnecting peer");
                // send one goodbye
                self.eth2_rpc
                    .shutdown(peer_id, RequestId::Behaviour, reason);
            }
        }
    }
}

/* Public API types */

/// The type of RPC requests the Behaviour informs it has received and allows for sending.
///
// NOTE: This is an application-level wrapper over the lower network level requests that can be
//       sent. The main difference is the absence of the Ping, Metadata and Goodbye protocols, which don't
//       leave the Behaviour. For all protocols managed by RPC see `RPCRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A Status message.
    Status(StatusMessage),
    /// A data by hash request.
    DataByHash(DataByHashRequest),
    /// An AnswerFile message.
    AnswerFile(ShardedFile),
    /// A GetChunks request.
    GetChunks(GetChunksRequest),
}

impl std::convert::From<Request> for OutboundRequest {
    fn from(req: Request) -> OutboundRequest {
        match req {
            Request::Status(s) => OutboundRequest::Status(s),
            Request::DataByHash(r) => OutboundRequest::DataByHash(r),
            Request::AnswerFile(r) => OutboundRequest::AnswerFile(r),
            Request::GetChunks(r) => OutboundRequest::GetChunks(r),
        }
    }
}

/// The type of RPC responses the Behaviour informs it has received, and allows for sending.
///
// NOTE: This is an application-level wrapper over the lower network level responses that can be
//       sent. The main difference is the absense of Pong and Metadata, which don't leave the
//       Behaviour. For all protocol reponses managed by RPC see `RPCResponse` and
//       `RPCCodedResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// A Status message.
    Status(StatusMessage),
    /// A response to a get DATA_BY_HASH request. A None response signals the end of the batch.
    DataByHash(Option<Box<ZgsData>>),
    /// A response to a GET_CHUNKS request.
    Chunks(ChunkArrayWithProof),
}

impl std::convert::From<Response> for RPCCodedResponse {
    fn from(resp: Response) -> RPCCodedResponse {
        match resp {
            Response::Status(s) => RPCCodedResponse::Success(RPCResponse::Status(s)),
            Response::DataByHash(r) => match r {
                Some(b) => RPCCodedResponse::Success(RPCResponse::DataByHash(b)),
                None => RPCCodedResponse::StreamTermination(ResponseTermination::DataByHash),
            },
            Response::Chunks(c) => RPCCodedResponse::Success(RPCResponse::Chunks(c)),
        }
    }
}
