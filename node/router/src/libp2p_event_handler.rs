use std::{ops::Neg, sync::Arc};

use chunk_pool::ChunkPoolMessage;
use file_location_cache::FileLocationCache;
use network::multiaddr::Protocol;
use network::types::{AnnounceShardConfig, SignedAnnounceShardConfig};
use network::Multiaddr;
use network::{
    rpc::StatusMessage,
    types::{
        AnnounceChunks, AnnounceFile, FindChunks, FindFile, HasSignature, SignedAnnounceChunks,
        SignedAnnounceFile, SignedMessage,
    },
    Keypair, MessageAcceptance, MessageId, NetworkGlobals, NetworkMessage, PeerId, PeerRequestId,
    PublicKey, PubsubMessage, Request, RequestId, Response,
};
use shared_types::{bytes_to_chunks, timestamp_now, TxID};
use storage::config::ShardConfig;
use storage_async::Store;
use sync::{SyncMessage, SyncSender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, RwLock};

use crate::peer_manager::PeerManager;
use crate::Config;

lazy_static::lazy_static! {
    pub static ref FIND_FILE_TIMEOUT: chrono::Duration = chrono::Duration::minutes(2);
    pub static ref ANNOUNCE_FILE_TIMEOUT: chrono::Duration = chrono::Duration::minutes(2);
    pub static ref ANNOUNCE_SHARD_CONFIG_TIMEOUT: chrono::Duration = chrono::Duration::minutes(2);
    pub static ref TOLERABLE_DRIFT: chrono::Duration = chrono::Duration::seconds(5);
}

#[allow(deprecated)]
fn duration_since(timestamp: u32) -> chrono::Duration {
    let timestamp = i64::from(timestamp);
    let timestamp = chrono::NaiveDateTime::from_timestamp_opt(timestamp, 0).expect("should fit");
    let now = chrono::Utc::now().naive_utc();
    now.signed_duration_since(timestamp)
}

fn peer_id_to_public_key(peer_id: &PeerId) -> Result<PublicKey, String> {
    // A libp2p peer id byte representation should be 2 length bytes + 4 protobuf bytes + compressed pk bytes
    // if generated from a PublicKey with Identity multihash.
    let pk_bytes = &peer_id.to_bytes()[2..];

    PublicKey::from_protobuf_encoding(pk_bytes).map_err(|e| {
        format!(
            " Cannot parse libp2p public key public key from peer id: {}",
            e
        )
    })
}

fn verify_signature(msg: &dyn HasSignature, peer_id: &PeerId, propagation_source: PeerId) -> bool {
    match peer_id_to_public_key(peer_id) {
        Ok(pub_key) => msg.verify_signature(&pub_key),
        Err(err) => {
            error!(
                ?err,
                ?peer_id,
                ?propagation_source,
                "Failed to verify signature"
            );
            false
        }
    }
}

pub struct Libp2pEventHandler {
    config: Config,
    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,
    /// A channel to the router service.
    network_send: mpsc::UnboundedSender<NetworkMessage>,
    /// A channel to the syncing service.
    sync_send: SyncSender,
    /// A channel to the RPC chunk pool service.
    chunk_pool_send: mpsc::UnboundedSender<ChunkPoolMessage>,
    /// Node keypair for signing messages.
    local_keypair: Keypair,
    /// Log and transaction storage.
    store: Store,
    /// Cache for storing and serving gossip messages.
    file_location_cache: Arc<FileLocationCache>,
    /// All connected peers.
    peers: Arc<RwLock<PeerManager>>,
}

impl Libp2pEventHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        network_globals: Arc<NetworkGlobals>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        sync_send: SyncSender,
        chunk_pool_send: UnboundedSender<ChunkPoolMessage>,
        local_keypair: Keypair,
        store: Store,
        file_location_cache: Arc<FileLocationCache>,
        peers: Arc<RwLock<PeerManager>>,
    ) -> Self {
        Self {
            config,
            network_globals,
            network_send,
            sync_send,
            chunk_pool_send,
            local_keypair,
            store,
            file_location_cache,
            peers,
        }
    }

    fn send_to_network(&self, message: NetworkMessage) {
        self.network_send.send(message).unwrap_or_else(|err| {
            warn!(%err, "Could not send message to the network service");
        });
    }

    pub fn send_to_sync(&self, message: SyncMessage) {
        self.sync_send.notify(message).unwrap_or_else(|err| {
            warn!(%err, "Could not send message to the sync service");
        });
    }

    pub fn send_to_chunk_pool(&self, message: ChunkPoolMessage) {
        self.chunk_pool_send.send(message).unwrap_or_else(|err| {
            warn!(%err, "Could not send message to the chunk pool service");
        });
    }

    pub fn publish(&self, msg: PubsubMessage) {
        self.send_to_network(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }

    pub fn send_status(&self, peer_id: PeerId) {
        let status_message = StatusMessage { data: 123 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status request");

        self.send_to_network(NetworkMessage::SendRequest {
            peer_id,
            request_id: RequestId::Router,
            request: Request::Status(status_message),
        });
    }

    pub async fn on_peer_connected(&self, peer_id: PeerId, outgoing: bool) {
        self.peers.write().await.add(peer_id, outgoing);

        if outgoing {
            self.send_status(peer_id);
            self.send_to_sync(SyncMessage::PeerConnected { peer_id });
        }
    }

    pub async fn on_peer_disconnected(&self, peer_id: PeerId) {
        self.peers.write().await.remove(&peer_id);
        self.send_to_sync(SyncMessage::PeerDisconnected { peer_id });
    }

    pub async fn on_rpc_request(
        &self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: Request,
    ) {
        self.peers.write().await.update(&peer_id);

        match request {
            Request::Status(status) => {
                self.on_status_request(peer_id, request_id, status);
            }
            Request::GetChunks(request) => {
                self.send_to_sync(SyncMessage::RequestChunks {
                    peer_id,
                    request_id,
                    request,
                });
            }
            Request::DataByHash(_) => {
                // ignore
            }
        }
    }

    fn on_status_request(&self, peer_id: PeerId, request_id: PeerRequestId, status: StatusMessage) {
        debug!(%peer_id, ?status, "Received Status request");

        let status_message = StatusMessage { data: 456 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status response");

        self.send_to_network(NetworkMessage::SendResponse {
            peer_id,
            id: request_id,
            response: Response::Status(status_message),
        });
    }

    pub async fn on_rpc_response(
        &self,
        peer_id: PeerId,
        request_id: RequestId,
        response: Response,
    ) {
        self.peers.write().await.update(&peer_id);

        match response {
            Response::Status(status_message) => {
                debug!(%peer_id, ?status_message, "Received Status response");
            }
            Response::Chunks(response) => {
                let request_id = match request_id {
                    RequestId::Sync(sync_id) => sync_id,
                    _ => unreachable!("All Chunks responses belong to sync"),
                };

                self.send_to_sync(SyncMessage::ChunksResponse {
                    peer_id,
                    request_id,
                    response,
                });
            }
            Response::DataByHash(_) => {
                // ignore
            }
        }
    }

    pub async fn on_rpc_error(&self, peer_id: PeerId, request_id: RequestId) {
        self.peers.write().await.update(&peer_id);

        // Check if the failed RPC belongs to sync
        if let RequestId::Sync(request_id) = request_id {
            self.send_to_sync(SyncMessage::RpcError {
                peer_id,
                request_id,
            });
        }
    }

    pub async fn on_pubsub_message(
        &self,
        propagation_source: PeerId,
        source: PeerId,
        id: &MessageId,
        message: PubsubMessage,
    ) -> MessageAcceptance {
        trace!(?message, %propagation_source, %source, %id, "Received pubsub message");

        match message {
            PubsubMessage::ExampleMessage(_) => MessageAcceptance::Ignore,
            PubsubMessage::FindFile(msg) => self.on_find_file(msg).await,
            PubsubMessage::FindChunks(msg) => self.on_find_chunks(msg).await,
            PubsubMessage::AnnounceFile(msg) => self.on_announce_file(propagation_source, msg),
            PubsubMessage::AnnounceChunks(msg) => self.on_announce_chunks(propagation_source, msg),
            PubsubMessage::AnnounceShardConfig(msg) => {
                self.on_announce_shard_config(propagation_source, msg)
            }
        }
    }

    async fn get_listen_addr_or_add(&self) -> Option<Multiaddr> {
        if let Some(addr) = self.get_listen_addr() {
            return Some(addr);
        }

        let ipv4_addr = public_ip::addr_v4().await?;

        let mut addr = Multiaddr::empty();
        addr.push(Protocol::Ip4(ipv4_addr));
        addr.push(Protocol::Tcp(self.network_globals.listen_port_tcp()));
        addr.push(Protocol::P2p(self.network_globals.local_peer_id().into()));

        self.network_globals
            .listen_multiaddrs
            .write()
            .insert(0, addr.clone());

        info!(
            ?addr,
            "Create public ip address to broadcase file announcement"
        );

        Some(addr)
    }

    fn get_listen_addr(&self) -> Option<Multiaddr> {
        let listen_addrs = self.network_globals.listen_multiaddrs.read();

        if self.config.private_ip_enabled {
            listen_addrs.first().cloned()
        } else {
            listen_addrs
                .iter()
                .find(|&x| Self::contains_public_ip(x))
                .cloned()
        }
    }

    fn contains_public_ip(addr: &Multiaddr) -> bool {
        for c in addr.iter() {
            match c {
                Protocol::Ip4(ip4_addr) => {
                    return !ip4_addr.is_broadcast()
                        && !ip4_addr.is_documentation()
                        && !ip4_addr.is_link_local()
                        && !ip4_addr.is_loopback()
                        && !ip4_addr.is_multicast()
                        && !ip4_addr.is_private()
                        && !ip4_addr.is_unspecified()
                }
                Protocol::Ip6(ip6_addr) => {
                    return !ip6_addr.is_loopback()
                        && !ip6_addr.is_multicast()
                        && !ip6_addr.is_unspecified()
                }
                _ => {}
            }
        }

        false
    }

    pub async fn construct_announce_file_message(&self, tx_id: TxID) -> Option<PubsubMessage> {
        let peer_id = *self.network_globals.peer_id.read();

        let addr = self.get_listen_addr_or_add().await?;

        let timestamp = timestamp_now();
        let shard_config = self.store.get_store().flow().get_shard_config();

        let msg = AnnounceFile {
            tx_id,
            num_shard: shard_config.num_shard,
            shard_id: shard_config.shard_id,
            peer_id: peer_id.into(),
            at: addr.into(),
            timestamp,
        };

        let mut signed = match SignedMessage::sign_message(msg, &self.local_keypair) {
            Ok(signed) => signed,
            Err(e) => {
                error!(%tx_id.seq, %e, "Failed to sign AnnounceFile message");
                return None;
            }
        };

        signed.resend_timestamp = timestamp;

        Some(PubsubMessage::AnnounceFile(signed))
    }

    pub async fn construct_announce_shard_config_message(
        &self,
        shard_config: ShardConfig,
    ) -> Option<PubsubMessage> {
        let peer_id = *self.network_globals.peer_id.read();
        let addr = self.get_listen_addr_or_add().await?;
        let timestamp = timestamp_now();

        let msg = AnnounceShardConfig {
            num_shard: shard_config.num_shard,
            shard_id: shard_config.shard_id,
            peer_id: peer_id.into(),
            at: addr.into(),
            timestamp,
        };

        let mut signed = match SignedMessage::sign_message(msg, &self.local_keypair) {
            Ok(signed) => signed,
            Err(e) => {
                error!(%e, "Failed to sign AnnounceShardConfig message");
                return None;
            }
        };

        signed.resend_timestamp = timestamp;

        Some(PubsubMessage::AnnounceShardConfig(signed))
    }

    async fn on_find_file(&self, msg: FindFile) -> MessageAcceptance {
        let FindFile { tx_id, timestamp } = msg;

        // verify timestamp
        let d = duration_since(timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *FIND_FILE_TIMEOUT {
            debug!(%timestamp, "Invalid timestamp, ignoring FindFile message");
            return MessageAcceptance::Ignore;
        }

        // check if we have it
        if matches!(self.store.check_tx_completed(tx_id.seq).await, Ok(true)) {
            if let Ok(Some(tx)) = self.store.get_tx_by_seq_number(tx_id.seq).await {
                if tx.id() == tx_id {
                    debug!(?tx_id, "Found file locally, responding to FindFile query");

                    return match self.construct_announce_file_message(tx_id).await {
                        Some(msg) => {
                            self.publish(msg);
                            MessageAcceptance::Ignore
                        }
                        // propagate FindFile query to other nodes
                        None => MessageAcceptance::Accept,
                    };
                }
            }
        }

        // try from cache
        if let Some(mut msg) = self.file_location_cache.get_one(tx_id) {
            debug!(?tx_id, "Found file in cache, responding to FindFile query");

            msg.resend_timestamp = timestamp_now();
            self.publish(PubsubMessage::AnnounceFile(msg));

            return MessageAcceptance::Ignore;
        }

        // propagate FindFile query to other nodes
        MessageAcceptance::Accept
    }

    pub async fn construct_announce_chunks_message(
        &self,
        tx_id: TxID,
        index_start: u64,
        index_end: u64,
    ) -> Option<PubsubMessage> {
        let peer_id = *self.network_globals.peer_id.read();
        let addr = self.get_listen_addr_or_add().await?;
        let timestamp = timestamp_now();

        let msg = AnnounceChunks {
            tx_id,
            index_start,
            index_end,
            peer_id: peer_id.into(),
            at: addr.into(),
            timestamp,
        };

        let mut signed = match SignedMessage::sign_message(msg, &self.local_keypair) {
            Ok(signed) => signed,
            Err(e) => {
                error!(%tx_id.seq, %e, "Failed to sign AnnounceChunks message");
                return None;
            }
        };

        signed.resend_timestamp = timestamp;

        Some(PubsubMessage::AnnounceChunks(signed))
    }

    async fn on_find_chunks(&self, msg: FindChunks) -> MessageAcceptance {
        // validate message
        if msg.index_start >= msg.index_end {
            debug!(?msg, "Invalid chunk index range");
            return MessageAcceptance::Reject;
        }

        // verify timestamp
        let d = duration_since(msg.timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *FIND_FILE_TIMEOUT {
            debug!(%msg.timestamp, "Invalid timestamp, ignoring FindFile message");
            return MessageAcceptance::Ignore;
        }

        // check if we have specified chunks even file not finalized yet
        // validate end index
        let tx = match self.store.get_tx_by_seq_number(msg.tx_id.seq).await {
            Ok(Some(tx)) if tx.id() == msg.tx_id => tx,
            _ => return MessageAcceptance::Accept,
        };

        // validate index range
        if let Ok(size) = usize::try_from(tx.size) {
            let num_chunks = bytes_to_chunks(size);
            if msg.index_end > num_chunks as u64 {
                debug!(?msg, "Invalid chunk end index for FindChunks message");
                return MessageAcceptance::Reject;
            }
        }

        // TODO(qhz): check if there is better way to check existence of requested chunks.
        match self
            .store
            .get_chunks_by_tx_and_index_range(
                msg.tx_id.seq,
                msg.index_start as usize,
                msg.index_end as usize,
            )
            .await
        {
            Ok(Some(_)) => (),
            _ => return MessageAcceptance::Accept,
        };

        debug!(?msg, "Found chunks to respond FindChunks message");

        match self
            .construct_announce_chunks_message(msg.tx_id, msg.index_start, msg.index_end)
            .await
        {
            Some(msg) => {
                self.publish(msg);
                MessageAcceptance::Ignore
            }
            // propagate FindFile query to other nodes
            None => MessageAcceptance::Accept,
        }
    }

    fn on_announce_file(
        &self,
        propagation_source: PeerId,
        msg: SignedAnnounceFile,
    ) -> MessageAcceptance {
        // verify message signature
        if !verify_signature(&msg, &msg.peer_id, propagation_source) {
            return MessageAcceptance::Reject;
        }

        // verify public ip address if required
        let addr = msg.at.clone().into();
        if !self.config.private_ip_enabled && !Self::contains_public_ip(&addr) {
            return MessageAcceptance::Reject;
        }

        // propagate gossip to peers
        let d = duration_since(msg.resend_timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *ANNOUNCE_FILE_TIMEOUT {
            debug!(%msg.resend_timestamp, "Invalid resend timestamp, ignoring AnnounceFile message");
            return MessageAcceptance::Ignore;
        }

        // notify sync layer
        self.send_to_sync(SyncMessage::AnnounceFileGossip {
            tx_id: msg.tx_id,
            peer_id: msg.peer_id.clone().into(),
            addr,
        });

        // insert message to cache
        self.file_location_cache.insert(msg);

        MessageAcceptance::Accept
    }

    fn on_announce_shard_config(
        &self,
        propagation_source: PeerId,
        msg: SignedAnnounceShardConfig,
    ) -> MessageAcceptance {
        // verify message signature
        if !verify_signature(&msg, &msg.peer_id, propagation_source) {
            return MessageAcceptance::Reject;
        }

        // verify public ip address if required
        let addr = msg.at.clone().into();
        if !self.config.private_ip_enabled && !Self::contains_public_ip(&addr) {
            return MessageAcceptance::Reject;
        }

        // propagate gossip to peers
        let d = duration_since(msg.resend_timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *ANNOUNCE_SHARD_CONFIG_TIMEOUT {
            debug!(%msg.resend_timestamp, "Invalid resend timestamp, ignoring AnnounceShardConfig message");
            return MessageAcceptance::Ignore;
        }

        let shard_config = ShardConfig {
            shard_id: msg.shard_id,
            num_shard: msg.num_shard,
        };
        // notify sync layer
        self.send_to_sync(SyncMessage::AnnounceShardConfig {
            shard_config,
            peer_id: msg.peer_id.clone().into(),
            addr,
        });

        // insert message to cache
        self.file_location_cache
            .insert_peer_config(msg.peer_id.clone().into(), shard_config);

        MessageAcceptance::Accept
    }

    fn on_announce_chunks(
        &self,
        propagation_source: PeerId,
        msg: SignedAnnounceChunks,
    ) -> MessageAcceptance {
        // verify message signature
        if !verify_signature(&msg, &msg.peer_id, propagation_source) {
            return MessageAcceptance::Reject;
        }

        // verify public ip address if required
        let addr = msg.at.clone().into();
        if !self.config.private_ip_enabled && !Self::contains_public_ip(&addr) {
            return MessageAcceptance::Reject;
        }

        // propagate gossip to peers
        let d = duration_since(msg.resend_timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *ANNOUNCE_FILE_TIMEOUT {
            debug!(%msg.resend_timestamp, "Invalid resend timestamp, ignoring AnnounceChunks message");
            return MessageAcceptance::Ignore;
        }

        // notify sync layer
        self.send_to_sync(SyncMessage::AnnounceChunksGossip { msg: msg.inner });

        MessageAcceptance::Accept
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use channel::Message::*;
    use file_location_cache::{test_util::AnnounceFileBuilder, FileLocationCache};
    use network::{
        discovery::{CombinedKey, ConnectionId},
        discv5::enr::EnrBuilder,
        rpc::{GetChunksRequest, StatusMessage, SubstreamId},
        types::FindFile,
        CombinedKeyExt, Keypair, MessageAcceptance, MessageId, Multiaddr, NetworkGlobals,
        NetworkMessage, PeerId, PubsubMessage, Request, RequestId, Response, SyncId,
    };
    use shared_types::{timestamp_now, ChunkArray, ChunkArrayWithProof, FlowRangeProof, TxID};
    use storage::{
        log_store::{log_manager::LogConfig, Store},
        LogManager,
    };
    use sync::{test_util::create_2_store, SyncMessage, SyncReceiver, SyncSender};
    use task_executor::test_utils::TestRuntime;
    use tokio::sync::{
        mpsc::{self, error::TryRecvError},
        RwLock,
    };

    use crate::{peer_manager::PeerManager, Config};

    use super::*;

    struct Context {
        runtime: TestRuntime,
        network_globals: Arc<NetworkGlobals>,
        keypair: Keypair,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        network_recv: mpsc::UnboundedReceiver<NetworkMessage>,
        sync_send: SyncSender,
        sync_recv: SyncReceiver,
        chunk_pool_send: mpsc::UnboundedSender<ChunkPoolMessage>,
        // chunk_pool_recv: mpsc::UnboundedReceiver<ChunkPoolMessage>,
        store: Arc<dyn Store>,
        file_location_cache: Arc<FileLocationCache>,
        peers: Arc<RwLock<PeerManager>>,
    }

    impl Default for Context {
        fn default() -> Self {
            let runtime = TestRuntime::default();
            let (network_globals, keypair) = Context::new_network_globals();
            let (network_send, network_recv) = mpsc::unbounded_channel();
            let (sync_send, sync_recv) = channel::Channel::unbounded();
            let (chunk_pool_send, _chunk_pool_recv) = mpsc::unbounded_channel();
            let store = LogManager::memorydb(LogConfig::default()).unwrap();
            Self {
                runtime,
                network_globals: Arc::new(network_globals),
                keypair,
                network_send,
                network_recv,
                sync_send,
                sync_recv,
                chunk_pool_send,
                // chunk_pool_recv,
                store: Arc::new(store),
                file_location_cache: Arc::new(FileLocationCache::default()),
                peers: Arc::new(RwLock::new(PeerManager::new(Config::default()))),
            }
        }
    }

    impl Context {
        fn new_handler(&self) -> Libp2pEventHandler {
            Libp2pEventHandler::new(
                Config::default().with_private_ip_enabled(true),
                self.network_globals.clone(),
                self.network_send.clone(),
                self.sync_send.clone(),
                self.chunk_pool_send.clone(),
                self.keypair.clone(),
                storage_async::Store::new(self.store.clone(), self.runtime.task_executor.clone()),
                self.file_location_cache.clone(),
                self.peers.clone(),
            )
        }

        fn new_network_globals() -> (NetworkGlobals, Keypair) {
            let keypair = Keypair::generate_secp256k1();
            let enr_key = CombinedKey::from_libp2p(&keypair).unwrap();
            let enr = EnrBuilder::new("v4").build(&enr_key).unwrap();
            let network_globals = NetworkGlobals::new(enr, 30000, 30000, vec![]);

            let listen_addr: Multiaddr = "/ip4/127.0.0.1/tcp/30000".parse().unwrap();
            network_globals.listen_multiaddrs.write().push(listen_addr);

            (network_globals, keypair)
        }

        fn assert_status_request(&mut self, expected_peer_id: PeerId) {
            match self.network_recv.try_recv() {
                Ok(NetworkMessage::SendRequest {
                    peer_id,
                    request,
                    request_id,
                }) => {
                    assert_eq!(peer_id, expected_peer_id);
                    assert!(matches!(request, Request::Status(..)));
                    assert!(matches!(request_id, RequestId::Router))
                }
                Ok(_) => panic!("Unexpected network message type received"),
                Err(e) => panic!("No network message received: {:?}", e),
            }
        }

        fn assert_file_announcement_published(&mut self, expected_tx_id: TxID) {
            match self.network_recv.try_recv() {
                Ok(NetworkMessage::Publish { messages }) => {
                    assert_eq!(messages.len(), 1);
                    assert!(
                        matches!(&messages[0], PubsubMessage::AnnounceFile(file) if file.tx_id == expected_tx_id)
                    );
                }
                Ok(_) => panic!("Unexpected network message type received"),
                Err(e) => panic!("No network message received: {:?}", e),
            }
        }
    }

    #[test]
    fn test_send_status() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert!(matches!(
            ctx.network_recv.try_recv(),
            Err(TryRecvError::Empty)
        ));

        let alice = PeerId::random();
        handler.send_status(alice);

        ctx.assert_status_request(alice);
    }

    #[tokio::test]
    async fn test_on_peer_connected_incoming() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert_eq!(handler.peers.read().await.size(), 0);

        let alice = PeerId::random();
        handler.on_peer_connected(alice, false).await;

        assert_eq!(handler.peers.read().await.size(), 1);
        assert!(matches!(
            ctx.network_recv.try_recv(),
            Err(TryRecvError::Empty)
        ));
        assert!(matches!(ctx.sync_recv.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_on_peer_connected_outgoing() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert_eq!(handler.peers.read().await.size(), 0);

        let alice = PeerId::random();
        handler.on_peer_connected(alice, true).await;

        assert_eq!(handler.peers.read().await.size(), 1);
        ctx.assert_status_request(alice);
        assert!(matches!(
            ctx.sync_recv.try_recv(),
            Ok(Notification(SyncMessage::PeerConnected {peer_id})) if peer_id == alice
        ));
    }

    #[tokio::test]
    async fn test_on_peer_disconnected() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        handler.on_peer_connected(alice, false).await;
        assert_eq!(handler.peers.read().await.size(), 1);

        handler.on_peer_disconnected(alice).await;
        assert_eq!(handler.peers.read().await.size(), 0);
        assert!(matches!(
            ctx.sync_recv.try_recv(),
            Ok(Notification(SyncMessage::PeerDisconnected {peer_id})) if peer_id == alice
        ));
    }

    #[tokio::test]
    async fn test_on_rpc_request_status() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let req_id = (ConnectionId::new(4), SubstreamId(12));
        let request = Request::Status(StatusMessage { data: 412 });
        handler.on_rpc_request(alice, req_id, request).await;

        match ctx.network_recv.try_recv() {
            Ok(NetworkMessage::SendResponse {
                peer_id,
                response,
                id,
            }) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(response, Response::Status(..)));
                assert_eq!(id, req_id);
            }
            Ok(_) => panic!("Unexpected network message type received"),
            Err(e) => panic!("No network message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_request_get_chunks() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = (ConnectionId::new(4), SubstreamId(12));
        let raw_request = GetChunksRequest {
            tx_id: TxID::random_hash(7),
            index_start: 66,
            index_end: 99,
        };
        handler
            .on_rpc_request(alice, id, Request::GetChunks(raw_request.clone()))
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::RequestChunks {
                peer_id,
                request_id,
                request,
            })) => {
                assert_eq!(peer_id, alice);
                assert_eq!(request_id, id);
                assert_eq!(request, raw_request);
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_response() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = TxID::random_hash(555);
        let data = ChunkArrayWithProof {
            chunks: ChunkArray {
                data: vec![1, 2, 3, 4],
                start_index: 16,
            },
            proof: FlowRangeProof::new_empty(),
        };
        handler
            .on_rpc_response(
                alice,
                RequestId::Sync(SyncId::SerialSync { tx_id: id }),
                Response::Chunks(data.clone()),
            )
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::ChunksResponse {
                peer_id,
                request_id,
                response,
            })) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(request_id, SyncId::SerialSync { tx_id } if tx_id == id ));
                assert_eq!(response, data);
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_error() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = TxID::random_hash(555);
        handler
            .on_rpc_error(alice, RequestId::Sync(SyncId::SerialSync { tx_id: id }))
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::RpcError {
                peer_id,
                request_id,
            })) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(request_id, SyncId::SerialSync { tx_id } if tx_id == id ));
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    async fn handle_find_file_msg(
        handler: &Libp2pEventHandler,
        tx_id: TxID,
        timestamp: u32,
    ) -> MessageAcceptance {
        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let message = PubsubMessage::FindFile(FindFile { tx_id, timestamp });
        handler.on_pubsub_message(alice, bob, &id, message).await
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_invalid_timestamp() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        // message too future
        let result = handle_find_file_msg(
            &handler,
            TxID::random_hash(412),
            timestamp_now() + 10 + TOLERABLE_DRIFT.num_seconds() as u32,
        )
        .await;
        assert!(matches!(result, MessageAcceptance::Ignore));

        // message too old
        let result = handle_find_file_msg(
            &handler,
            TxID::random_hash(412),
            timestamp_now() - 10 - FIND_FILE_TIMEOUT.num_seconds() as u32,
        )
        .await;
        assert!(matches!(result, MessageAcceptance::Ignore));
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_not_found() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        let result = handle_find_file_msg(&handler, TxID::random_hash(412), timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Accept));
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_in_store() {
        let mut ctx = Context::default();

        // prepare store with txs
        let (_, store, txs, _) = create_2_store(vec![1314]);
        ctx.store = store;

        let handler = ctx.new_handler();

        // receive find file request
        let result = handle_find_file_msg(&handler, txs[0].id(), timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Ignore));
        ctx.assert_file_announcement_published(txs[0].id());
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_in_cache() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        // prepare tx in cache
        let tx_id = TxID::random_hash(412);
        let signed = AnnounceFileBuilder::default()
            .with_tx_id(tx_id)
            .with_timestamp(timestamp_now() - 5)
            .build();
        ctx.file_location_cache.insert(signed);

        // receive find file request
        let result = handle_find_file_msg(&handler, tx_id, timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Ignore));
        ctx.assert_file_announcement_published(tx_id);
    }

    #[tokio::test]
    async fn test_on_pubsub_announce_file_invalid_sig() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let tx_id = TxID::random_hash(412);

        // change signed message
        let message = match handler
            .construct_announce_file_message(tx_id)
            .await
            .unwrap()
        {
            PubsubMessage::AnnounceFile(mut file) => {
                let malicious_addr: Multiaddr = "/ip4/127.0.0.38/tcp/30000".parse().unwrap();
                file.inner.at = malicious_addr.into();
                PubsubMessage::AnnounceFile(file)
            }
            _ => panic!("Unexpected pubsub message type"),
        };

        // failed to verify signature
        let result = handler.on_pubsub_message(alice, bob, &id, message).await;
        assert!(matches!(result, MessageAcceptance::Reject));
    }

    #[tokio::test]
    async fn test_on_pubsub_announce_file() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        // prepare message
        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let tx = TxID::random_hash(412);
        let message = handler.construct_announce_file_message(tx).await.unwrap();

        // succeeded to handle
        let result = handler.on_pubsub_message(alice, bob, &id, message).await;
        assert!(matches!(result, MessageAcceptance::Accept));

        // ensure notify to sync layer
        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::AnnounceFileGossip {
                tx_id,
                peer_id,
                addr,
            })) => {
                assert_eq!(tx_id, tx);
                assert_eq!(peer_id, *ctx.network_globals.peer_id.read());
                assert_eq!(
                    addr,
                    *ctx.network_globals
                        .listen_multiaddrs
                        .read()
                        .first()
                        .unwrap()
                );
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }

        // ensure cache updated
        assert_eq!(ctx.file_location_cache.get_all(tx).len(), 1);
    }
}
