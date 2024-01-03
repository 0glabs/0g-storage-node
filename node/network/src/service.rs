use crate::behaviour::{Behaviour, BehaviourEvent, PeerRequestId, Request, Response};
use crate::config::NetworkLoad;
use crate::discovery::enr;
use crate::multiaddr::Protocol;
use crate::rpc::{GoodbyeReason, RPCResponseErrorCode, ReqId};
use crate::types::{error, GossipKind};
use crate::{EnrExt, NetworkMessage};
use crate::{NetworkConfig, NetworkGlobals, PeerAction, ReportSource};
use futures::prelude::*;
use libp2p::core::{
    identity::Keypair, multiaddr::Multiaddr, muxing::StreamMuxerBox, transport::Boxed,
};
use libp2p::{
    bandwidth::{BandwidthLogging, BandwidthSinks},
    core, noise,
    swarm::{ConnectionLimits, SwarmBuilder, SwarmEvent},
    PeerId, Swarm, Transport,
};
use std::fs::File;
use std::io::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

use crate::peer_manager::{MIN_OUTBOUND_ONLY_FACTOR, PEER_EXCESS_FACTOR, PRIORITY_PEER_EXCESS};

pub const NETWORK_KEY_FILENAME: &str = "key";
/// The maximum simultaneous libp2p connections per peer.
const MAX_CONNECTIONS_PER_PEER: u32 = 1;

/// The types of events than can be obtained from polling the libp2p service.
///
/// This is a subset of the events that a libp2p swarm emits.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Libp2pEvent<AppReqId: ReqId> {
    /// A behaviour event
    Behaviour(BehaviourEvent<AppReqId>),
    /// A new listening address has been established.
    NewListenAddr(Multiaddr),
    /// We reached zero listening addresses.
    ZeroListeners,
}

/// The configuration and state of the libp2p components for the beacon node.
pub struct Service<AppReqId: ReqId> {
    /// The libp2p Swarm handler.
    pub swarm: Swarm<Behaviour<AppReqId>>,
    /// The bandwidth logger for the underlying libp2p transport.
    pub bandwidth: Arc<BandwidthSinks>,
    /// This node's PeerId.
    pub local_peer_id: PeerId,
}

pub struct Context<'a> {
    pub config: &'a NetworkConfig,
}

impl<AppReqId: ReqId> Service<AppReqId> {
    pub async fn new(
        executor: task_executor::TaskExecutor,
        network_sender: UnboundedSender<NetworkMessage>,
        ctx: Context<'_>,
    ) -> error::Result<(Arc<NetworkGlobals>, Keypair, Self)> {
        trace!("Libp2p Service starting");

        let config = ctx.config;
        // initialise the node's ID
        let local_keypair = load_private_key(config);

        // Create an ENR or load from disk if appropriate
        let enr = enr::build_or_load_enr(local_keypair.clone(), config)?;

        let local_peer_id = enr.peer_id();

        // set up a collection of variables accessible outside of the network crate
        let network_globals = Arc::new(NetworkGlobals::new(
            enr.clone(),
            config.libp2p_port,
            config.discovery_port,
            config
                .trusted_peers
                .iter()
                .map(|x| PeerId::from(x.clone()))
                .collect(),
        ));

        // try and construct UPnP port mappings if required.
        if let Some(upnp_config) = crate::nat::UPnPConfig::from_config(config) {
            if config.upnp_enabled {
                executor.spawn_blocking(
                    move || crate::nat::construct_upnp_mappings(upnp_config, network_sender),
                    "UPnP",
                );
            }
        }

        info!(
            peer_id = %enr.peer_id(),
            bandwidth_config = %format!("{}-{}", config.network_load, NetworkLoad::from(config.network_load).name),
            "Libp2p Starting",
        );

        let discovery_string = if config.disable_discovery {
            "None".into()
        } else {
            config.discovery_port.to_string()
        };

        debug!(
            address = ?config.listen_address,
            tcp_port = %config.libp2p_port,
            udp_port = %discovery_string,
            "Attempting to open listening ports",
        );

        let (mut swarm, bandwidth) = {
            // Set up the transport - tcp/ws with noise and mplex
            let (transport, bandwidth) = build_transport(local_keypair.clone())
                .map_err(|e| format!("Failed to build transport: {:?}", e))?;

            // Lighthouse network behaviour
            let behaviour =
                Behaviour::new(&local_keypair.clone(), ctx, network_globals.clone()).await?;

            // use the executor for libp2p
            struct Executor(task_executor::TaskExecutor);
            impl libp2p::core::Executor for Executor {
                fn exec(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
                    self.0.spawn(f, "libp2p");
                }
            }

            // sets up the libp2p connection limits
            let limits = ConnectionLimits::default()
                .with_max_pending_incoming(Some(5))
                .with_max_pending_outgoing(Some(16))
                .with_max_established_incoming(Some(
                    (config.target_peers as f32
                        * (1.0 + PEER_EXCESS_FACTOR - MIN_OUTBOUND_ONLY_FACTOR))
                        .ceil() as u32,
                ))
                .with_max_established_outgoing(Some(
                    (config.target_peers as f32 * (1.0 + PEER_EXCESS_FACTOR)).ceil() as u32,
                ))
                .with_max_established(Some(
                    (config.target_peers as f32 * (1.0 + PEER_EXCESS_FACTOR + PRIORITY_PEER_EXCESS))
                        .ceil() as u32,
                ))
                .with_max_established_per_peer(Some(MAX_CONNECTIONS_PER_PEER));

            (
                SwarmBuilder::new(transport, behaviour, local_peer_id)
                    .notify_handler_buffer_size(std::num::NonZeroUsize::new(7).expect("Not zero"))
                    .connection_event_buffer_size(64)
                    .connection_limits(limits)
                    .executor(Box::new(Executor(executor)))
                    .build(),
                bandwidth,
            )
        };

        // listen on the specified address
        let listen_multiaddr = {
            let mut m = Multiaddr::from(config.listen_address);
            m.push(Protocol::Tcp(config.libp2p_port));
            m
        };

        match Swarm::listen_on(&mut swarm, listen_multiaddr.clone()) {
            Ok(_) => {
                let mut log_address = listen_multiaddr;
                log_address.push(Protocol::P2p(local_peer_id.into()));
                info!(address = %log_address, "Listening established");
            }
            Err(err) => {
                error!(
                    error = ?err,
                    listen_multiaddr = %listen_multiaddr,
                    "Unable to listen on libp2p address",
                );
                return Err("Libp2p was unable to listen on the given listen address.".into());
            }
        };

        // helper closure for dialing peers
        let mut dial = |multiaddr: Multiaddr| {
            // strip the p2p protocol if it exists
            match Swarm::dial(&mut swarm, multiaddr.clone()) {
                Ok(()) => debug!(address = %multiaddr, "Dialing libp2p peer"),
                Err(err) => {
                    debug!(address = %multiaddr, error = ?err, "Could not connect to peer")
                }
            };
        };

        // attempt to connect to user-input libp2p nodes
        for multiaddr in &config.libp2p_nodes {
            dial(multiaddr.clone());
        }

        // attempt to connect to any specified boot-nodes
        let mut boot_nodes = config.boot_nodes_enr.clone();
        boot_nodes.dedup();

        for bootnode_enr in boot_nodes {
            for multiaddr in &bootnode_enr.multiaddr() {
                // ignore udp multiaddr if it exists
                let components = multiaddr.iter().collect::<Vec<_>>();
                if let Protocol::Udp(_) = components[1] {
                    continue;
                }

                if !network_globals
                    .peers
                    .read()
                    .is_connected_or_dialing(&bootnode_enr.peer_id())
                {
                    dial(multiaddr.clone());
                }
            }
        }

        for multiaddr in &config.boot_nodes_multiaddr {
            // check TCP support for dialing
            if multiaddr
                .iter()
                .any(|proto| matches!(proto, Protocol::Tcp(_)))
            {
                dial(multiaddr.clone());
            }
        }

        let mut subscribed_topics: Vec<GossipKind> = vec![];

        // for topic_kind in &config.topics {
        //     if swarm.behaviour_mut().subscribe_kind(topic_kind.clone()) {
        //         subscribed_topics.push(topic_kind.clone());
        //     } else {
        //         warn!(topic = ?topic_kind, "Could not subscribe to topic");
        //     }
        // }

        for topic_kind in &crate::types::CORE_TOPICS {
            if swarm.behaviour_mut().subscribe_kind(topic_kind.clone()) {
                subscribed_topics.push(topic_kind.clone());
            } else {
                warn!(topic = ?topic_kind, "Could not subscribe to topic");
            }
        }

        if !subscribed_topics.is_empty() {
            info!(topics = ?subscribed_topics, "Subscribed to topics");
        }

        let service = Service {
            swarm,
            bandwidth,
            local_peer_id,
        };

        Ok((network_globals, local_keypair, service))
    }

    /// Sends a request to a peer, with a given Id.
    pub fn send_request(&mut self, peer_id: PeerId, request_id: AppReqId, request: Request) {
        self.swarm
            .behaviour_mut()
            .send_request(peer_id, request_id, request);
    }

    /// Informs the peer that their request failed.
    pub fn respond_with_error(
        &mut self,
        peer_id: PeerId,
        id: PeerRequestId,
        error: RPCResponseErrorCode,
        reason: String,
    ) {
        self.swarm
            .behaviour_mut()
            .send_error_reponse(peer_id, id, error, reason);
    }

    /// Report a peer's action.
    pub fn report_peer(
        &mut self,
        peer_id: &PeerId,
        action: PeerAction,
        source: ReportSource,
        msg: &'static str,
    ) {
        self.swarm
            .behaviour_mut()
            .peer_manager_mut()
            .report_peer(peer_id, action, source, None, msg);
    }

    /// Disconnect and ban a peer, providing a reason.
    pub fn goodbye_peer(&mut self, peer_id: &PeerId, reason: GoodbyeReason, source: ReportSource) {
        self.swarm
            .behaviour_mut()
            .goodbye_peer(peer_id, reason, source);
    }

    /// Sends a response to a peer's request.
    pub fn send_response(&mut self, peer_id: PeerId, id: PeerRequestId, response: Response) {
        self.swarm
            .behaviour_mut()
            .send_successful_response(peer_id, id, response);
    }

    pub async fn next_event(&mut self) -> Libp2pEvent<AppReqId> {
        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::Behaviour(behaviour) => {
                    // Handle banning here
                    match &behaviour {
                        BehaviourEvent::PeerBanned(peer_id) => {
                            self.swarm.ban_peer_id(*peer_id);
                        }
                        BehaviourEvent::PeerUnbanned(peer_id) => {
                            self.swarm.unban_peer_id(*peer_id);
                        }
                        _ => {}
                    }
                    return Libp2pEvent::Behaviour(behaviour);
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id: _,
                    endpoint: _,
                    num_established: _,
                    concurrent_dial_errors: _,
                } => {}
                SwarmEvent::ConnectionClosed {
                    peer_id: _,
                    cause: _,
                    endpoint: _,
                    num_established: _,
                } => {}
                SwarmEvent::NewListenAddr { address, .. } => {
                    return Libp2pEvent::NewListenAddr(address)
                }
                SwarmEvent::IncomingConnection {
                    local_addr,
                    send_back_addr,
                } => {
                    trace!(our_addr = %local_addr, from = %send_back_addr, "Incoming connection")
                }
                SwarmEvent::IncomingConnectionError {
                    local_addr,
                    send_back_addr,
                    error,
                } => {
                    debug!(our_addr = %local_addr, from = %send_back_addr, error = %error, "Failed incoming connection");
                }
                SwarmEvent::BannedPeer { peer_id, .. } => {
                    debug!(peer_id = %peer_id, "Banned peer connection rejected");
                }
                SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                    debug!(peer_id = ?peer_id,  error = %error, "Failed to dial address");
                }
                SwarmEvent::ExpiredListenAddr { address, .. } => {
                    debug!(address = %address, "Listen address expired")
                }
                SwarmEvent::ListenerClosed {
                    addresses, reason, ..
                } => {
                    error!(addresses = ?addresses, reason = ?reason, "Listener closed");
                    if Swarm::listeners(&self.swarm).count() == 0 {
                        return Libp2pEvent::ZeroListeners;
                    }
                }
                SwarmEvent::ListenerError { error, .. } => {
                    // this is non fatal, but we still check
                    warn!(error = ?error, "Listener error");
                    if Swarm::listeners(&self.swarm).count() == 0 {
                        return Libp2pEvent::ZeroListeners;
                    }
                }
                SwarmEvent::Dialing(_peer_id) => {}
            }
        }
    }
}

type BoxedTransport = Boxed<(PeerId, StreamMuxerBox)>;

/// The implementation supports TCP/IP, WebSockets over TCP/IP, noise as the encryption layer, and
/// mplex as the multiplexing layer.
fn build_transport(
    local_private_key: Keypair,
) -> std::io::Result<(BoxedTransport, Arc<BandwidthSinks>)> {
    let tcp = libp2p::tcp::TokioTcpConfig::new().nodelay(true);
    let transport = libp2p::dns::TokioDnsConfig::system(tcp)?;
    #[cfg(feature = "libp2p-websocket")]
    let transport = {
        let trans_clone = transport.clone();
        transport.or_transport(libp2p::websocket::WsConfig::new(trans_clone))
    };

    let (transport, bandwidth) = BandwidthLogging::new(transport);

    // mplex config
    let mut mplex_config = libp2p::mplex::MplexConfig::new();
    mplex_config.set_max_buffer_size(256);
    mplex_config.set_max_buffer_behaviour(libp2p::mplex::MaxBufferBehaviour::Block);

    // yamux config
    let mut yamux_config = libp2p::yamux::YamuxConfig::default();
    yamux_config.set_window_update_mode(libp2p::yamux::WindowUpdateMode::on_read());

    // Authentication
    Ok((
        transport
            .upgrade(core::upgrade::Version::V1)
            .authenticate(generate_noise_config(&local_private_key))
            .multiplex(core::upgrade::SelectUpgrade::new(
                yamux_config,
                mplex_config,
            ))
            .timeout(Duration::from_secs(10))
            .boxed(),
        bandwidth,
    ))
}

// Useful helper functions for debugging. Currently not used in the client.
#[allow(dead_code)]
fn keypair_from_hex(hex_bytes: &str) -> error::Result<Keypair> {
    let hex_bytes = if let Some(stripped) = hex_bytes.strip_prefix("0x") {
        stripped.to_string()
    } else {
        hex_bytes.to_string()
    };

    hex::decode(hex_bytes)
        .map_err(|e| format!("Failed to parse p2p secret key bytes: {:?}", e).into())
        .and_then(keypair_from_bytes)
}

#[allow(dead_code)]
fn keypair_from_bytes(mut bytes: Vec<u8>) -> error::Result<Keypair> {
    libp2p::core::identity::secp256k1::SecretKey::from_bytes(&mut bytes)
        .map(|secret| {
            let keypair: libp2p::core::identity::secp256k1::Keypair = secret.into();
            Keypair::Secp256k1(keypair)
        })
        .map_err(|e| format!("Unable to parse p2p secret key: {:?}", e).into())
}

/// Loads a private key from disk. If this fails, a new key is
/// generated and is then saved to disk.
///
/// Currently only secp256k1 keys are allowed, as these are the only keys supported by discv5.
pub fn load_private_key(config: &NetworkConfig) -> Keypair {
    // check for key from disk
    let network_key_f = config.network_dir.join(NETWORK_KEY_FILENAME);
    if let Ok(mut network_key_file) = File::open(network_key_f.clone()) {
        let mut key_bytes: Vec<u8> = Vec::with_capacity(36);
        match network_key_file.read_to_end(&mut key_bytes) {
            Err(_) => debug!("Could not read network key file"),
            Ok(_) => {
                // only accept secp256k1 keys for now
                if let Ok(secret_key) =
                    libp2p::core::identity::secp256k1::SecretKey::from_bytes(&mut key_bytes)
                {
                    let kp: libp2p::core::identity::secp256k1::Keypair = secret_key.into();
                    debug!("Loaded network key from disk.");
                    return Keypair::Secp256k1(kp);
                } else {
                    debug!("Network key file is not a valid secp256k1 key");
                }
            }
        }
    }

    // if a key could not be loaded from disk, generate a new one and save it
    let local_private_key = Keypair::generate_secp256k1();
    if let Keypair::Secp256k1(key) = local_private_key.clone() {
        let _ = std::fs::create_dir_all(&config.network_dir);
        match File::create(network_key_f.clone())
            .and_then(|mut f| f.write_all(&key.secret().to_bytes()))
        {
            Ok(_) => {
                debug!("New network key generated and written to disk");
            }
            Err(e) => {
                warn!(
                    "Could not write node key to file: {:?}. error: {}",
                    network_key_f, e
                );
            }
        }
    }
    local_private_key
}

/// Generate authenticated XX Noise config from identity keys
fn generate_noise_config(
    identity_keypair: &Keypair,
) -> noise::NoiseAuthenticated<noise::XX, noise::X25519Spec, ()> {
    let static_dh_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(identity_keypair)
        .expect("signing can fail only once during starting a node");
    noise::NoiseConfig::xx(static_dh_keys).into_authenticated()
}
