use crate::metrics;
use crate::rate_limit::PubsubRateLimiter;
use crate::Config;
use crate::{libp2p_event_handler::Libp2pEventHandler, peer_manager::PeerManager};
use chunk_pool::ChunkPoolMessage;
use file_location_cache::FileLocationCache;
use futures::{channel::mpsc::Sender, prelude::*};
use miner::MinerMessage;
use network::rpc::GoodbyeReason;
use network::types::GossipKind;
use network::{
    BehaviourEvent, Keypair, Libp2pEvent, NetworkGlobals, NetworkMessage, NetworkReceiver,
    NetworkSender, PubsubMessage, RequestId, Service as LibP2PService, Swarm,
};
use network::{MessageAcceptance, PeerAction, PeerId, ReportSource};
use pruner::PrunerMessage;
use shared_types::ShardedFile;
use std::sync::Arc;
use std::time::Duration;
use storage::log_store::Store as LogStore;
use storage_async::Store;
use sync::{SyncMessage, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::interval;

/// Service that handles communication between internal services and the libp2p service.
pub struct RouterService {
    config: Config,

    /// The underlying libp2p service that drives all the network interactions.
    libp2p: LibP2PService<RequestId>,

    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,

    /// The receiver channel for Zgs to communicate with the network service.
    network_recv: NetworkReceiver,

    /// The receiver channel for Zgs to communicate with the pruner service.
    pruner_recv: Option<mpsc::UnboundedReceiver<PrunerMessage>>,

    /// All connected peers.
    peers: Arc<RwLock<PeerManager>>,

    /// Handler for libp2p events.
    libp2p_event_handler: Libp2pEventHandler,

    /// Stores potentially created UPnP mappings to be removed on shutdown. (TCP port and UDP
    /// port).
    upnp_mappings: (Option<u16>, Option<u16>),

    store: Arc<dyn LogStore>,

    pubsub_rate_limiter: PubsubRateLimiter,
}

impl RouterService {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        libp2p: LibP2PService<RequestId>,
        network_globals: Arc<NetworkGlobals>,
        network_recv: NetworkReceiver,
        network_send: NetworkSender,
        sync_send: SyncSender,
        _miner_send: Option<broadcast::Sender<MinerMessage>>,
        chunk_pool_send: UnboundedSender<ChunkPoolMessage>,
        pruner_recv: Option<mpsc::UnboundedReceiver<PrunerMessage>>,
        store: Arc<dyn LogStore>,
        file_location_cache: Arc<FileLocationCache>,
        local_keypair: Keypair,
        config: Config,
    ) -> Result<(), String> {
        let peers = Arc::new(RwLock::new(PeerManager::new(config.clone())));

        let pubsub_rate_limiter = PubsubRateLimiter::new(100, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::Example, 10, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::NewFile, 50, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::AskFile, 50, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::FindFile, 10, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::AnnounceFile, 10, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::FindChunks, 10, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::AnnounceChunks, 10, Duration::from_secs(10))?
            .limit_by_topic(GossipKind::AnnounceShardConfig, 50, Duration::from_secs(10))?;

        // create the network service and spawn the task
        let router = RouterService {
            config: config.clone(),
            libp2p,
            network_globals: network_globals.clone(),
            network_recv,
            pruner_recv,
            peers: peers.clone(),
            libp2p_event_handler: Libp2pEventHandler::new(
                config,
                network_globals,
                network_send,
                sync_send,
                chunk_pool_send,
                local_keypair,
                Store::new(store.clone(), executor.clone()),
                file_location_cache,
                peers,
            ),
            upnp_mappings: (None, None),
            store,
            pubsub_rate_limiter,
        };

        // spawn service
        let shutdown_sender = executor.shutdown_sender();

        executor.spawn(router.main(shutdown_sender), "router");

        Ok(())
    }

    async fn main(mut self, mut shutdown_sender: Sender<ShutdownReason>) {
        let mut heartbeat_service = interval(self.config.heartbeat_interval);
        let mut heartbeat_batcher = interval(self.config.batcher_timeout);
        let mut heartbeat_rate_limiter = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // handle a message sent to the network
                Some(msg) = self.network_recv.recv() => self.on_network_msg(msg, &mut shutdown_sender).await,

                // handle event coming from the network
                event = self.libp2p.next_event() => self.on_libp2p_event(event, &mut shutdown_sender).await,

                Some(msg) = Self::try_recv(&mut self.pruner_recv) => self.on_pruner_msg(msg).await,

                // heartbeat for service
                _ = heartbeat_service.tick() => self.on_heartbeat().await,

                // heartbeat for expire file batcher
                _ = heartbeat_batcher.tick() => self.libp2p_event_handler.expire_batcher().await,

                _ = heartbeat_rate_limiter.tick() => self.pubsub_rate_limiter.prune(),
            }
        }
    }

    async fn try_recv<T>(maybe_recv: &mut Option<mpsc::UnboundedReceiver<T>>) -> Option<T> {
        match maybe_recv {
            None => None,
            Some(recv) => recv.recv().await,
        }
    }

    /// Handle an event received from the network.
    async fn on_libp2p_event(
        &mut self,
        ev: Libp2pEvent<RequestId>,
        shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        trace!(?ev, "Received new event from libp2p");

        match ev {
            Libp2pEvent::Behaviour(event) => match event {
                BehaviourEvent::PeerConnectedOutgoing(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_connected(peer_id, true)
                        .await;
                }
                BehaviourEvent::PeerConnectedIncoming(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_connected(peer_id, false)
                        .await;
                }
                BehaviourEvent::PeerBanned(_) | BehaviourEvent::PeerUnbanned(_) => {
                    // No action required for these events.
                }
                BehaviourEvent::PeerDisconnected(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_disconnected(peer_id)
                        .await;
                }
                BehaviourEvent::RequestReceived {
                    peer_id,
                    id,
                    request,
                } => {
                    if self.network_globals.peers.read().is_connected(&peer_id) {
                        self.libp2p_event_handler
                            .on_rpc_request(peer_id, id, request)
                            .await;
                    } else {
                        debug!(%peer_id, ?request, "Dropping request of disconnected peer");
                    }
                }
                BehaviourEvent::ResponseReceived {
                    peer_id,
                    id,
                    response,
                } => {
                    self.libp2p_event_handler
                        .on_rpc_response(peer_id, id, response)
                        .await;
                }
                BehaviourEvent::RPCFailed { id, peer_id } => {
                    self.libp2p_event_handler.on_rpc_error(peer_id, id).await;
                }
                BehaviourEvent::StatusPeer(peer_id) => {
                    self.libp2p_event_handler.send_status(peer_id);
                }
                BehaviourEvent::PubsubMessage {
                    id,
                    propagation_source,
                    source,
                    message,
                    ..
                } => {
                    let result = if let Err((rate_limit_kind, _)) = self
                        .pubsub_rate_limiter
                        .allows(&propagation_source, &message)
                    {
                        warn!(%propagation_source, kind=?message.kind(), ?rate_limit_kind, "Pubsub message rate limited");
                        self.libp2p_event_handler
                            .send_to_network(NetworkMessage::ReportPeer {
                                peer_id: propagation_source,
                                action: PeerAction::LowToleranceError,
                                source: ReportSource::Gossipsub,
                                msg: "Pubsub message rate limited",
                            });
                        MessageAcceptance::Reject
                    } else {
                        self.libp2p_event_handler
                            .on_pubsub_message(propagation_source, source, &id, message)
                            .await
                    };

                    self.libp2p
                        .swarm
                        .behaviour_mut()
                        .report_message_validation_result(&propagation_source, id, result);
                }
            },
            Libp2pEvent::NewListenAddr(multiaddr) => {
                info!(?multiaddr, "New listen address");
                self.network_globals
                    .listen_multiaddrs
                    .write()
                    .push(multiaddr);
            }
            Libp2pEvent::ZeroListeners => {
                let _ = shutdown_sender
                    .send(ShutdownReason::Failure(
                        "All listeners are closed. Unable to listen",
                    ))
                    .await
                    .map_err(|e| {
                        warn!(
                            error = %e,
                            "failed to send a shutdown signal",
                        )
                    });
            }
        }
    }

    /// Handle a message sent to the network service.
    async fn on_network_msg(
        &mut self,
        msg: NetworkMessage,
        _shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        trace!(?msg, "Received new message");

        metrics::SERVICE_ROUTE_NETWORK_MESSAGE.mark(1);

        match msg {
            NetworkMessage::SendRequest {
                peer_id,
                request,
                request_id,
            } => {
                self.libp2p.send_request(peer_id, request_id, request);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_SEND_REQUEST.mark(1);
            }
            NetworkMessage::SendResponse {
                peer_id,
                response,
                id,
            } => {
                self.libp2p.send_response(peer_id, id, response);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_SEND_RESPONSE.mark(1);
            }
            NetworkMessage::SendErrorResponse {
                peer_id,
                error,
                id,
                reason,
            } => {
                self.libp2p.respond_with_error(peer_id, id, error, reason);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_SEND_ERROR_RESPONSE.mark(1);
            }
            NetworkMessage::Publish { messages } => {
                if self.libp2p.swarm.connected_peers().next().is_none() {
                    // this is a broadcast message, when current node doesn't have any peers connected, try to connect any peer in config
                    for multiaddr in &self.config.libp2p_nodes {
                        match Swarm::dial(&mut self.libp2p.swarm, multiaddr.clone()) {
                            Ok(()) => {
                                debug!(address = %multiaddr, "Dialing libp2p peer");
                                break;
                            }
                            Err(err) => {
                                debug!(address = %multiaddr, error = ?err, "Could not connect to peer");
                            }
                        };
                    }
                }

                let mut topic_kinds = Vec::new();
                for message in &messages {
                    if !topic_kinds.contains(&message.kind()) {
                        topic_kinds.push(message.kind());
                    }
                }
                debug!(
                    count = messages.len(),
                    topics = ?topic_kinds,
                    "Sending pubsub messages",
                );
                self.libp2p.swarm.behaviour_mut().publish(messages);

                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_PUBLISH.mark(1);
            }
            NetworkMessage::ReportPeer {
                peer_id,
                action,
                source,
                msg,
            } => {
                self.libp2p.report_peer(&peer_id, action, source, msg);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_REPORT_PEER.mark(1);
            }
            NetworkMessage::GoodbyePeer {
                peer_id,
                reason,
                source,
            } => {
                self.libp2p.goodbye_peer(&peer_id, reason, source);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_GOODBYE_PEER.mark(1);
            }
            NetworkMessage::DialPeer { address, peer_id } => {
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_DIAL_PEER.mark(1);

                if self.libp2p.swarm.is_connected(&peer_id) {
                    self.libp2p_event_handler
                        .send_to_sync(SyncMessage::PeerConnected { peer_id });
                    metrics::SERVICE_ROUTE_NETWORK_MESSAGE_DIAL_PEER_ALREADY.mark(1);
                } else {
                    match Swarm::dial(&mut self.libp2p.swarm, address.clone()) {
                        Ok(()) => {
                            debug!(%address, "Dialing libp2p peer");
                            metrics::SERVICE_ROUTE_NETWORK_MESSAGE_DIAL_PEER_NEW_OK.mark(1);
                        }
                        Err(err) => {
                            info!(%address, error = ?err, "Failed to dial peer");
                            self.libp2p_event_handler
                                .send_to_sync(SyncMessage::DialFailed { peer_id, err });
                            metrics::SERVICE_ROUTE_NETWORK_MESSAGE_DIAL_PEER_NEW_FAIL.mark(1);
                        }
                    };
                }
            }
            NetworkMessage::DisconnectPeer { peer_id } => {
                self.disconnect_peer(peer_id);
            }
            NetworkMessage::AnnounceLocalFile { tx_id } => {
                let new_file = ShardedFile {
                    tx_id,
                    shard_config: self.store.get_shard_config().into(),
                };
                let msg = PubsubMessage::NewFile(new_file.into());
                self.libp2p.swarm.behaviour_mut().publish(vec![msg]);
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_ANNOUNCE_LOCAL_FILE.mark(1);
                debug!(?new_file, "Publish NewFile message");
            }
            NetworkMessage::UPnPMappingEstablished {
                tcp_socket,
                udp_socket,
            } => {
                metrics::SERVICE_ROUTE_NETWORK_MESSAGE_UPNP.mark(1);
                self.upnp_mappings = (tcp_socket.map(|s| s.port()), udp_socket.map(|s| s.port()));
                // If there is an external TCP port update, modify our local ENR.
                if let Some(tcp_socket) = tcp_socket {
                    if let Err(e) = self
                        .libp2p
                        .swarm
                        .behaviour_mut()
                        .discovery_mut()
                        .update_enr_tcp_port(tcp_socket.port())
                    {
                        warn!(error = %e, "Failed to update ENR");
                    }
                }
                if let Some(udp_socket) = udp_socket {
                    if let Err(e) = self
                        .libp2p
                        .swarm
                        .behaviour_mut()
                        .discovery_mut()
                        .update_enr_udp_socket(udp_socket)
                    {
                        warn!(error = %e, "Failed to update ENR");
                    }
                }
            }
        }
    }

    async fn on_pruner_msg(&mut self, msg: PrunerMessage) {
        match msg {
            PrunerMessage::ChangeShardConfig(shard_config) => {
                self.libp2p_event_handler
                    .send_to_chunk_pool(ChunkPoolMessage::ChangeShardConfig(shard_config));

                let shard_config = shared_types::ShardConfig::from(shard_config);
                self.libp2p_event_handler
                    .publish(PubsubMessage::AnnounceShardConfig(shard_config.into()));
            }
        }
    }

    async fn on_heartbeat(&mut self) {
        let expired_peers = self.peers.write().await.expired_peers();

        let num_expired_peers = expired_peers.len() as u64;
        metrics::SERVICE_EXPIRED_PEERS.update(num_expired_peers);
        if num_expired_peers > 0 {
            debug!(%num_expired_peers, "Heartbeat, remove expired peers")
        }

        for peer_id in expired_peers {
            self.disconnect_peer(peer_id);
        }
    }

    fn disconnect_peer(&mut self, peer_id: PeerId) {
        let pm = self.libp2p.swarm.behaviour_mut().peer_manager_mut();
        if pm.is_connected(&peer_id) {
            pm.disconnect_peer(peer_id, GoodbyeReason::IrrelevantNetwork);
        }
    }
}

impl Drop for RouterService {
    fn drop(&mut self) {
        info!("Router service shutdown");
        // attempt to remove port mappings
        network::nat::remove_mappings(self.upnp_mappings.0, self.upnp_mappings.1);
    }
}
