use crate::context::SyncNetworkContext;
use crate::controllers::peers::{PeerState, SyncPeers};
use crate::controllers::{FileSyncGoal, FileSyncInfo};
use crate::InstantWrapper;
use file_location_cache::FileLocationCache;
use libp2p::swarm::DialError;
use network::types::FindChunks;
use network::{
    multiaddr::Protocol, rpc::GetChunksRequest, types::FindFile, Multiaddr, NetworkMessage,
    PeerAction, PeerId, PubsubMessage, SyncId as RequestId,
};
use rand::Rng;
use shared_types::{timestamp_now, ChunkArrayWithProof, TxID, CHUNK_SIZE};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use storage::log_store::log_manager::PORA_CHUNK_SIZE;
use storage_async::Store;

pub const MAX_CHUNKS_TO_REQUEST: u64 = 2 * 1024;
const MAX_REQUEST_FAILURES: usize = 100;
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5);
const WAIT_OUTGOING_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const NEXT_REQUEST_WAIT_TIME: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureReason {
    DBError(String),
    TxReverted(TxID),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyncState {
    Idle,
    FindingPeers {
        origin: InstantWrapper,
        since: InstantWrapper,
    },
    FoundPeers,
    ConnectingPeers {
        since: InstantWrapper,
    },
    AwaitingOutgoingConnection {
        since: InstantWrapper,
    },
    AwaitingDownload {
        since: InstantWrapper,
    },
    Downloading {
        peer_id: PeerId,
        from_chunk: u64,
        to_chunk: u64,
        since: InstantWrapper,
    },
    Completed,
    Failed {
        reason: FailureReason,
    },
}

pub struct SerialSyncController {
    // only used for log purpose
    tx_seq: u64,

    /// The unique transaction ID.
    tx_id: TxID,

    tx_start_chunk_in_flow: u64,

    since: InstantWrapper,

    /// File sync goal.
    goal: FileSyncGoal,

    /// The next chunk id that we need to retrieve.
    next_chunk: u64,

    /// Continuous RPC failures to request chunks.
    failures: usize,

    /// Current state of this request.
    state: SyncState,

    /// Sync peer manager.
    peers: SyncPeers,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// Log and transaction storage.
    store: Store,

    /// Cache for storing and serving gossip messages.
    file_location_cache: Arc<FileLocationCache>,
}

impl SerialSyncController {
    pub fn new(
        tx_id: TxID,
        tx_start_chunk_in_flow: u64,
        goal: FileSyncGoal,
        ctx: Arc<SyncNetworkContext>,
        store: Store,
        file_location_cache: Arc<FileLocationCache>,
    ) -> Self {
        SerialSyncController {
            tx_seq: tx_id.seq,
            tx_id,
            tx_start_chunk_in_flow,
            since: Instant::now().into(),
            goal,
            next_chunk: goal.index_start,
            failures: 0,
            state: SyncState::Idle,
            peers: SyncPeers::new(ctx.clone(), tx_id, file_location_cache.clone()),
            ctx,
            store,
            file_location_cache,
        }
    }

    pub fn get_sync_info(&self) -> FileSyncInfo {
        FileSyncInfo {
            elapsed_secs: self.since.elapsed().as_secs(),
            peers: self.peers.states(),
            goal: self.goal,
            next_chunks: self.next_chunk,
            state: format!("{:?}", self.state),
        }
    }

    pub fn get_status(&self) -> &SyncState {
        &self.state
    }

    pub fn is_completed_or_failed(&self) -> bool {
        matches!(self.state, SyncState::Completed | SyncState::Failed { .. })
    }

    /// Resets the status to re-sync file when failed.
    pub fn reset(&mut self, maybe_range: Option<(u64, u64)>) {
        if let Some((start, end)) = maybe_range {
            // Sync new chunks regardless of previously downloaded file or chunks.
            // It's up to client to avoid duplicated chunks sync.
            self.goal = FileSyncGoal::new(self.goal.num_chunks, start, end);
            self.next_chunk = start;
        } else if self.goal.is_all_chunks() {
            // retry the failed file sync at break point
            debug!(%self.tx_seq, %self.next_chunk, "Continue to sync failed file");
        } else {
            // Ignore the failed chunks sync, and change to file sync.
            self.goal = FileSyncGoal::new_file(self.goal.num_chunks);
            self.next_chunk = 0;
        }

        self.failures = 0;
        self.state = SyncState::Idle;
        // remove disconnected peers
        self.peers.transition();
    }

    fn try_find_peers(&mut self) {
        info!(%self.tx_seq, "Finding peers");

        if self.goal.is_all_chunks() {
            self.publish_find_file();
        } else {
            self.publish_find_chunks();
        }

        self.state = SyncState::FindingPeers {
            origin: self.since,
            since: Instant::now().into(),
        };
    }

    fn publish_find_file(&mut self) {
        // try from cache
        let mut found_new_peer = false;

        for announcement in self.file_location_cache.get_all(self.tx_id) {
            // make sure peer_id is part of the address
            let peer_id: PeerId = announcement.peer_id.clone().into();
            let mut addr: Multiaddr = announcement.at.clone().into();
            addr.push(Protocol::P2p(peer_id.into()));
            found_new_peer = self.on_peer_found(peer_id, addr) || found_new_peer;
        }

        if found_new_peer
            && self.peers.all_shards_available(vec![
                PeerState::Found,
                PeerState::Connecting,
                PeerState::Connected,
            ])
        {
            return;
        }

        self.ctx.publish(PubsubMessage::FindFile(FindFile {
            tx_id: self.tx_id,
            timestamp: timestamp_now(),
        }));
    }

    fn publish_find_chunks(&self) {
        self.ctx.publish(PubsubMessage::FindChunks(FindChunks {
            tx_id: self.tx_id,
            index_start: self.goal.index_start,
            index_end: self.goal.index_end,
            timestamp: timestamp_now(),
        }));
    }

    fn try_connect(&mut self) {
        // select a random peer
        while !self
            .peers
            .all_shards_available(vec![PeerState::Connecting, PeerState::Connected])
        {
            let (peer_id, address) = match self.peers.random_peer(PeerState::Found) {
                Some((peer_id, address)) => (peer_id, address),
                None => {
                    // peer may be disconnected by remote node and need to find peers again
                    warn!(%self.tx_seq, "No peers available to connect");
                    self.state = SyncState::Idle;
                    return;
                }
            };

            // connect to peer
            info!(%self.tx_seq, %peer_id, %address, "Attempting to connect to peer");
            self.ctx.send(NetworkMessage::DialPeer { address, peer_id });

            self.peers
                .update_state(&peer_id, PeerState::Found, PeerState::Connecting);
        }
        self.state = SyncState::ConnectingPeers {
            since: Instant::now().into(),
        };
    }

    fn try_request_next(&mut self) {
        // request next chunk array
        let from_chunk = self.next_chunk;
        let to_chunk = std::cmp::min(from_chunk + PORA_CHUNK_SIZE as u64, self.goal.index_end);
        let request_id = network::RequestId::Sync(RequestId::SerialSync { tx_id: self.tx_id });
        let request = GetChunksRequest {
            tx_id: self.tx_id,
            index_start: from_chunk,
            index_end: to_chunk,
        };

        // select a random peer
        let peer_id = match self.select_peer_for_request(&request) {
            Some(peer_id) => peer_id,
            None => {
                warn!(%self.tx_seq, "No peers available to request chunks");
                self.state = SyncState::Idle;
                return;
            }
        };

        self.ctx.send(NetworkMessage::SendRequest {
            peer_id,
            request_id,
            request: network::Request::GetChunks(request),
        });
        self.state = SyncState::Downloading {
            peer_id,
            from_chunk,
            to_chunk,
            since: Instant::now().into(),
        };
    }

    fn ban_peer(&mut self, peer_id: PeerId, reason: &'static str) {
        self.ctx.ban_peer(peer_id, reason);

        self.peers
            .update_state(&peer_id, PeerState::Connected, PeerState::Disconnecting);
    }

    pub fn on_peer_found(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        if let Some(shard_config) = self.file_location_cache.get_peer_config(&peer_id) {
            if self
                .peers
                .add_new_peer_with_config(peer_id, addr.clone(), shard_config)
            {
                info!(%self.tx_seq, %peer_id, %addr, "Found new peer");
                true
            } else {
                // e.g. multiple `AnnounceFile` messages propagated
                debug!(%self.tx_seq, %peer_id, %addr, "Found an existing peer");
                false
            }
        } else {
            debug!(%self.tx_seq, %peer_id, %addr, "No shard config found");
            false
        }
    }

    pub fn on_dail_failed(&mut self, peer_id: PeerId, err: &DialError) {
        match err {
            DialError::ConnectionLimit(_) => {
                if let Some(true) =
                    self.peers
                        .update_state(&peer_id, PeerState::Connecting, PeerState::Found)
                {
                    info!(%self.tx_seq, %peer_id, "Failed to dail peer due to outgoing connection limitation");
                    self.state = SyncState::AwaitingOutgoingConnection {
                        since: Instant::now().into(),
                    };
                }
            }
            _ => {
                if let Some(true) = self.peers.update_state(
                    &peer_id,
                    PeerState::Connecting,
                    PeerState::Disconnected,
                ) {
                    info!(%self.tx_seq, %peer_id, "Failed to dail peer");
                    self.state = SyncState::Idle;
                }
            }
        }
    }

    pub fn on_peer_connected(&mut self, peer_id: PeerId) {
        if let Some(true) =
            self.peers
                .update_state(&peer_id, PeerState::Connecting, PeerState::Connected)
        {
            info!(%self.tx_seq, %peer_id, "Peer connected");
        }
    }

    pub fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        match self
            .peers
            .update_state_force(&peer_id, PeerState::Disconnected)
        {
            Some(PeerState::Disconnecting) => info!(%self.tx_seq, %peer_id, "Peer disconnected"),
            Some(old_state) => {
                info!(%self.tx_seq, %peer_id, ?old_state, "Peer disconnected by remote");
            }
            None => {}
        }
    }

    /// Handle the case that got an unexpected response:
    /// 1. not in `Downloading` sync state.
    /// 2. from unexpected peer.
    fn handle_on_response_mismatch(&self, from_peer_id: PeerId) -> bool {
        match self.state {
            SyncState::Downloading { peer_id, .. } => {
                if from_peer_id == peer_id {
                    return false;
                }

                // got response from wrong peer
                // this can happen if we get a response for a timeout request
                warn!(%self.tx_seq, %from_peer_id, %peer_id, "Got response from unexpected peer");
                self.ctx.report_peer(
                    from_peer_id,
                    PeerAction::LowToleranceError,
                    "Peer id mismatch",
                );
                true
            }
            _ => {
                // FIXME(zz). Delayed response can enter this.
                warn!(%self.tx_seq, %from_peer_id, ?self.state, "Got response in unexpected state");
                self.ctx.report_peer(
                    from_peer_id,
                    PeerAction::HighToleranceError,
                    "Sync state mismatch",
                );
                true
            }
        }
    }

    pub async fn on_response(&mut self, from_peer_id: PeerId, response: ChunkArrayWithProof) {
        if self.handle_on_response_mismatch(from_peer_id) {
            return;
        }

        let (from_chunk, to_chunk) = match self.state {
            SyncState::Downloading {
                peer_id: _peer_id,
                from_chunk,
                to_chunk,
                ..
            } => (from_chunk, to_chunk),
            _ => return,
        };

        debug_assert!(from_chunk < to_chunk, "Invalid chunk boundaries");

        // invalid chunk array size: ban and re-request
        let data_len = response.chunks.data.len();
        if data_len == 0 || data_len % CHUNK_SIZE > 0 {
            warn!(%from_peer_id, %self.tx_seq, %data_len, "Invalid chunk response data length");
            self.ban_peer(from_peer_id, "Invalid chunk response data length");
            self.state = SyncState::Idle;
            return;
        }

        // invalid chunk range: ban and re-request
        let start_index = response.chunks.start_index;
        let end_index = start_index + (data_len / CHUNK_SIZE) as u64;
        if start_index != from_chunk || end_index != to_chunk {
            // FIXME(zz): Possible for relayed response.
            warn!(%self.tx_seq, "Invalid chunk response range, expected={from_chunk}..{to_chunk}, actual={start_index}..{end_index}");
            // self.ban_peer(from_peer_id, "Invalid chunk response range");
            // self.state = SyncState::Idle;
            return;
        }

        // validate Merkle proofs
        let validation_result = self
            .store
            .get_store()
            .validate_and_insert_range_proof(self.tx_seq, &response);

        match validation_result {
            Ok(true) => {}
            Ok(false) => {
                info!(%self.tx_seq, "Failed to validate chunks response due to no root found");
                self.state = SyncState::AwaitingDownload {
                    since: (Instant::now() + NEXT_REQUEST_WAIT_TIME).into(),
                };
                return;
            }
            Err(err) => {
                warn!(%err, %self.tx_seq, "Failed to validate chunks response");
                self.ban_peer(from_peer_id, "Chunk array validation failed");
                self.state = SyncState::Idle;
                return;
            }
        }

        self.failures = 0;

        let shard_config = self.store.get_store().flow().get_shard_config();
        let next_chunk = shard_config.next_segment_index(
            (from_chunk / PORA_CHUNK_SIZE as u64) as usize,
            (self.tx_start_chunk_in_flow / PORA_CHUNK_SIZE as u64) as usize,
        ) * PORA_CHUNK_SIZE;
        // store in db
        match self
            .store
            .put_chunks_with_tx_hash(self.tx_id.seq, self.tx_id.hash, response.chunks, None)
            .await
        {
            Ok(true) => self.next_chunk = next_chunk as u64,
            Ok(false) => {
                warn!(%self.tx_seq, ?self.tx_id, "Transaction reverted while storing chunks");
                self.state = SyncState::Failed {
                    reason: FailureReason::TxReverted(self.tx_id),
                };
                return;
            }
            Err(err) => {
                error!(%err, %self.tx_seq, "Unexpected DB error while storing chunks");
                self.state = SyncState::Failed {
                    reason: FailureReason::DBError(err.to_string()),
                };
                return;
            }
        }

        // prepare to download next
        if self.next_chunk < self.goal.index_end {
            self.state = SyncState::Idle;
            return;
        }

        // completed to download chunks
        if !self.goal.is_all_chunks() {
            self.state = SyncState::Completed;
            return;
        }

        // finalize tx if all chunks downloaded
        match self
            .store
            .finalize_tx_with_hash(self.tx_id.seq, self.tx_id.hash)
            .await
        {
            Ok(true) => self.state = SyncState::Completed,
            Ok(false) => {
                warn!(?self.tx_id, %self.tx_seq, "Transaction reverted during finalize_tx");
                self.state = SyncState::Failed {
                    reason: FailureReason::TxReverted(self.tx_id),
                };
            }
            Err(err) => {
                error!(%err, %self.tx_seq, "Unexpected error during finalize_tx");
                self.state = SyncState::Failed {
                    reason: FailureReason::DBError(err.to_string()),
                };
            }
        }
    }

    pub fn on_request_failed(&mut self, peer_id: PeerId) {
        if self.handle_on_response_mismatch(peer_id) {
            return;
        }

        self.handle_response_failure(peer_id, "RPC Error");
    }

    fn handle_response_failure(&mut self, peer_id: PeerId, reason: &'static str) {
        info!(%peer_id, %self.tx_seq, %reason, "Chunks request failed");

        // ban peer on too many failures
        // FIXME(zz): If remote removes a file, we will also get failure here.
        // self.ctx
        //     .report_peer(peer_id, PeerAction::HighToleranceError, reason);

        self.failures += 1;

        if self.failures <= MAX_REQUEST_FAILURES {
            // try again
            self.state = SyncState::AwaitingDownload {
                since: (Instant::now() + NEXT_REQUEST_WAIT_TIME).into(),
            };
        } else {
            // ban and find new peer to download
            self.ban_peer(peer_id, reason);
            self.state = SyncState::Idle;
        }
    }

    fn select_peer_for_request(&self, request: &GetChunksRequest) -> Option<PeerId> {
        let segment_index =
            (request.index_start + self.tx_start_chunk_in_flow) / PORA_CHUNK_SIZE as u64;
        let mut peers = self.peers.filter_peers(vec![PeerState::Connected]);

        peers.retain(|peer_id| match self.peers.shard_config(peer_id) {
            Some(v) => v.in_range(segment_index),
            None => false,
        });

        let len = peers.len();
        if len == 0 {
            return None;
        }

        let index = rand::thread_rng().gen_range(0..len);
        Some(peers[index])
    }

    pub fn transition(&mut self) {
        use PeerState::*;

        debug!(%self.tx_seq, ?self.state, "transition started");

        // update peer connection states
        self.peers.transition();

        let mut completed = false;

        while !completed {
            match self.state {
                SyncState::Idle => {
                    if self
                        .peers
                        .all_shards_available(vec![Found, Connecting, Connected])
                    {
                        self.state = SyncState::FoundPeers;
                    } else {
                        self.try_find_peers();
                    }
                }

                SyncState::FindingPeers { since, .. } => {
                    if self
                        .peers
                        .all_shards_available(vec![Found, Connecting, Connected])
                    {
                        self.state = SyncState::FoundPeers;
                    } else {
                        // storage node may not have the specific file when `FindFile`
                        // gossip message received. In this case, just broadcast the
                        // `FindFile` message again.
                        if since.elapsed() >= PEER_REQUEST_TIMEOUT {
                            debug!(%self.tx_seq, "Finding peer timeout and try to find peers again");
                            self.try_find_peers();
                        }

                        completed = true;
                    }
                }

                SyncState::FoundPeers => {
                    if self.peers.all_shards_available(vec![Connecting, Connected]) {
                        self.state = SyncState::ConnectingPeers {
                            since: Instant::now().into(),
                        };
                    } else {
                        self.try_connect();
                    }
                }

                SyncState::ConnectingPeers { .. } => {
                    if self.peers.all_shards_available(vec![Connected]) {
                        self.state = SyncState::AwaitingDownload {
                            since: Instant::now().into(),
                        };
                    } else if self.peers.count(&[Connecting]) == 0 {
                        debug!(%self.tx_seq, "Connecting to peers timeout and try to find other peers to dial");
                        self.state = SyncState::Idle;
                    } else {
                        // peers.transition() will handle the case that peer connecting timeout
                        completed = true;
                    }
                }

                SyncState::AwaitingOutgoingConnection { since } => {
                    if since.elapsed() < WAIT_OUTGOING_CONNECTION_TIMEOUT {
                        completed = true;
                    } else {
                        debug!(%self.tx_seq, "Waiting for outgoing connection timeout and try to find other peers to dial");
                        self.state = SyncState::Idle;
                    }
                }

                SyncState::AwaitingDownload { since } => {
                    if Instant::now() < since.0 {
                        completed = true;
                    } else {
                        self.try_request_next();
                    }
                }

                SyncState::Downloading { peer_id, since, .. } => {
                    if !matches!(self.peers.peer_state(&peer_id), Some(PeerState::Connected)) {
                        // e.g. peer disconnected by remote node
                        debug!(%self.tx_seq, "No peer to continue downloading and try to find other peers to download");
                        self.state = SyncState::Idle;
                    } else if since.elapsed() >= DOWNLOAD_TIMEOUT {
                        self.handle_response_failure(peer_id, "RPC timeout");
                    } else {
                        completed = true;
                    }
                }

                SyncState::Completed | SyncState::Failed { .. } => completed = true,
            }
        }

        debug!(%self.tx_seq, ?self.state, "transition ended");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::create_2_store;
    use crate::test_util::tests::create_file_location_cache;
    use libp2p::identity;
    use network::{ReportSource, Request};
    use storage::log_store::log_manager::LogConfig;
    use storage::log_store::log_manager::LogManager;
    use storage::log_store::LogStoreRead;
    use storage::H256;
    use task_executor::{test_utils::TestRuntime, TaskExecutor};
    use tokio::sync::mpsc::{self, UnboundedReceiver};

    #[test]
    fn test_status() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, None);

        assert_eq!(*controller.get_status(), SyncState::Idle);
        controller.state = SyncState::Completed;
        assert_eq!(*controller.get_status(), SyncState::Completed);

        controller.reset(None);
        assert_eq!(*controller.get_status(), SyncState::Idle);
    }

    #[tokio::test]
    async fn test_find_peers() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        assert_eq!(controller.peers.count(&[PeerState::Found]), 0);

        controller.try_find_peers();
        assert!(matches!(
            *controller.get_status(),
            SyncState::FindingPeers { .. }
        ));
        assert_eq!(controller.peers.count(&[PeerState::Found]), 1);
        assert!(network_recv.try_recv().is_err());

        controller.try_find_peers();
        assert_eq!(controller.peers.count(&[PeerState::Found]), 1);

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::Publish { messages } => {
                    assert_eq!(messages.len(), 1);

                    match &messages[0] {
                        PubsubMessage::FindFile(data) => {
                            assert_eq!(data.tx_id, controller.tx_id);
                        }
                        _ => {
                            panic!("Unexpected message type");
                        }
                    }
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_find_peers_not_in_file_cache() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.tx_seq = 1;
        controller.tx_id = TxID {
            seq: 1,
            hash: H256::random(),
        };
        controller.try_find_peers();

        assert_eq!(controller.peers.count(&[PeerState::Found]), 0);

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::Publish { messages } => {
                    assert_eq!(messages.len(), 1);

                    match &messages[0] {
                        PubsubMessage::FindFile(data) => {
                            assert_eq!(data.tx_id, controller.tx_id);
                        }
                        _ => {
                            panic!("Unexpected message type");
                        }
                    }
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }

        assert!(matches!(
            *controller.get_status(),
            SyncState::FindingPeers { .. }
        ));
    }

    #[tokio::test]
    async fn test_connect_peers() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.state = SyncState::FoundPeers;
        controller.try_connect();
        assert_eq!(controller.state, SyncState::Idle);
        assert!(network_recv.try_recv().is_err());

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        controller.peers.add_new_peer(new_peer_id, addr.clone());
        controller.try_connect();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::DialPeer { address, peer_id } => {
                    assert_eq!(address, addr);
                    assert_eq!(peer_id, new_peer_id);
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::DialPeer");
                }
            }
        }

        assert!(matches!(
            controller.state,
            SyncState::ConnectingPeers { .. }
        ));
    }

    #[tokio::test]
    async fn test_request_chunks() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.state = SyncState::AwaitingDownload {
            since: Instant::now().into(),
        };
        controller.try_request_next();
        assert_eq!(controller.state, SyncState::Idle);
        assert!(network_recv.try_recv().is_err());

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        controller.peers.add_new_peer(new_peer_id, addr.clone());
        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Connected);

        controller.try_request_next();
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::SendRequest {
                    peer_id,
                    request_id,
                    request,
                } => {
                    assert_eq!(peer_id, new_peer_id);
                    assert_eq!(
                        request,
                        Request::GetChunks(GetChunksRequest {
                            tx_id: controller.tx_id,
                            index_start: 0,
                            index_end: 123,
                        })
                    );

                    match request_id {
                        network::RequestId::Sync(sync_id) => match sync_id {
                            network::SyncId::SerialSync { tx_id } => {
                                assert_eq!(tx_id, controller.tx_id);
                            }
                        },
                        _ => {
                            panic!("Not expected message: network::RequestId::Sync");
                        }
                    }
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::SendRequest");
                }
            }
        }

        assert!(matches!(
            *controller.get_status(),
            SyncState::Downloading { .. }
        ));
    }

    #[tokio::test]
    async fn test_ban_peer() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller.ban_peer(new_peer_id, "unit test");

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, new_peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "unit test");
                }
                _ => {
                    panic!("Not received expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_report_peer() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (controller, mut network_recv) = create_default_controller(task_executor, None);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller
            .ctx
            .report_peer(new_peer_id, PeerAction::MidToleranceError, "unit test");

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, new_peer_id);
                    match action {
                        PeerAction::MidToleranceError => {}
                        _ => {
                            panic!("PeerAction expect MidToleranceError");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "unit test");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[test]
    fn test_peer_connected() {
        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, Some(new_peer_id));

        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        controller.peers.add_new_peer(new_peer_id, addr);

        controller.on_peer_connected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Found)
        );

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Connecting);
        controller.on_peer_connected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Connected)
        );
    }

    #[test]
    fn test_peer_disconnected() {
        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, Some(new_peer_id));

        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        controller.peers.add_new_peer(new_peer_id, addr);

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Disconnecting);
        controller.on_peer_disconnected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Disconnected)
        );

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Found);
        controller.on_peer_disconnected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Disconnected)
        );

        let new_peer_id_1 = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller.on_peer_disconnected(new_peer_id_1);
        assert_eq!(controller.peers.peer_state(&new_peer_id_1), None);
    }

    // FIXME(zz): enable.
    // #[tokio::test]
    #[allow(unused)]
    async fn test_response_mismatch_state_mismatch() {
        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (controller, mut network_recv) =
            create_default_controller(task_executor, Some(init_peer_id));

        assert!(controller.handle_on_response_mismatch(init_peer_id));

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    match action {
                        PeerAction::HighToleranceError => {}
                        _ => {
                            panic!("PeerAction expect HighToleranceError");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "Sync state mismatch");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_mismatch_peer_id_mismatch() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) =
            create_default_controller(task_executor, Some(peer_id));

        let peer_id_1 = identity::Keypair::generate_ed25519().public().to_peer_id();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: 1,
            since: Instant::now().into(),
        };
        assert!(controller.handle_on_response_mismatch(peer_id_1));
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id_1);
                    match action {
                        PeerAction::LowToleranceError => {}
                        _ => {
                            panic!("PeerAction expect MidToleranceError");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "Peer id mismatch");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Invalid chunk boundaries")]
    #[ignore = "only panic in debug mode"]
    async fn test_response_panic() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: 0,
            since: Instant::now().into(),
        };
        controller.on_response(peer_id, chunks).await;
    }

    #[tokio::test]
    async fn test_response_chunk_len_invalid() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let mut chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count as u64,
            since: Instant::now().into(),
        };

        chunks.chunks.data = Vec::new();
        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "Invalid chunk response data length");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    // FIXME(zz): enable.
    // #[tokio::test]
    #[allow(unused)]
    async fn test_response_chunk_index_invalid() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 1,
            to_chunk: chunk_count as u64,
            since: Instant::now().into(),
        };

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "Invalid chunk response range");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_validate_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count as u64,
            since: Instant::now().into(),
        };

        controller.tx_seq = 1;
        controller.tx_id = TxID {
            seq: 1,
            hash: H256::random(),
        };

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }

                    assert_eq!(msg, "Chunk array validation failed");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    // FIXME(zz): enable.
    // #[tokio::test]
    #[allow(unused)]
    async fn test_response_put_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (_, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            peer_store.clone(),
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count as u64,
            since: Instant::now().into(),
        };

        controller.on_response(peer_id, chunks).await;
        match controller.get_status() {
            SyncState::Failed { reason } => {
                assert!(matches!(reason, FailureReason::DBError(..)));
            }
            _ => {
                panic!("Not expected SyncState");
            }
        }

        assert!(network_recv.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_response_finalize_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 1025;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, 1024)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: 1024,
            since: Instant::now().into(),
        };

        controller.goal.num_chunks = 1024;
        controller.goal.index_end = 1024;

        controller.on_response(peer_id, chunks).await;
        match controller.get_status() {
            SyncState::Failed { reason } => {
                assert!(matches!(reason, FailureReason::DBError(..)));
            }
            state => {
                panic!("Not expected SyncState, {:?}", state);
            }
        }

        assert!(network_recv.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_response_success() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count as u64,
            since: Instant::now().into(),
        };

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Completed);
        assert!(network_recv.try_recv().is_err());
    }

    // FIXME(zz): enable.
    // #[tokio::test]
    #[allow(unused)]
    async fn test_handle_response_failure() {
        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let chunk_count = 123;
        let (store, _, txs, _) = create_2_store(vec![chunk_count]);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(init_peer_id),
            store,
            txs[0].id(),
            chunk_count,
        );

        for i in 0..(MAX_REQUEST_FAILURES + 1) {
            controller.handle_response_failure(init_peer_id, "unit test");
            if let Some(msg) = network_recv.recv().await {
                match msg {
                    NetworkMessage::ReportPeer {
                        peer_id,
                        action,
                        source,
                        msg,
                    } => {
                        assert_eq!(peer_id, init_peer_id);
                        match action {
                            PeerAction::LowToleranceError => {}
                            _ => {
                                panic!("PeerAction expect LowToleranceError");
                            }
                        }

                        match source {
                            ReportSource::SyncService => {}
                            _ => {
                                panic!("ReportSource expect SyncService");
                            }
                        }

                        assert_eq!(msg, "unit test");
                    }
                    _ => {
                        panic!("Not expected message: NetworkMessage::ReportPeer");
                    }
                }
            }

            assert_eq!(controller.failures, i + 1);
            if i == MAX_REQUEST_FAILURES {
                assert_eq!(*controller.get_status(), SyncState::Idle);

                if let Some(msg) = network_recv.recv().await {
                    match msg {
                        NetworkMessage::ReportPeer {
                            peer_id,
                            action,
                            source,
                            msg,
                        } => {
                            assert_eq!(peer_id, init_peer_id);
                            match action {
                                PeerAction::Fatal => {}
                                _ => {
                                    panic!("PeerAction expect Fatal");
                                }
                            }

                            match source {
                                ReportSource::SyncService => {}
                                _ => {
                                    panic!("ReportSource expect SyncService");
                                }
                            }

                            assert_eq!(msg, "unit test");
                        }
                        _ => {
                            panic!("Not expected message: NetworkMessage::ReportPeer");
                        }
                    }
                }
            } else {
                assert!(matches!(
                    *controller.get_status(),
                    SyncState::AwaitingDownload { .. }
                ));
            }
        }
    }

    fn create_default_controller(
        task_executor: TaskExecutor,
        peer_id: Option<PeerId>,
    ) -> (SerialSyncController, UnboundedReceiver<NetworkMessage>) {
        let tx_id = TxID {
            seq: 0,
            hash: H256::random(),
        };
        let num_chunks = 123;

        let config = LogConfig::default();
        let store = Arc::new(LogManager::memorydb(config).unwrap());

        create_controller(task_executor, peer_id, store, tx_id, num_chunks)
    }

    fn create_controller(
        task_executor: TaskExecutor,
        peer_id: Option<PeerId>,
        store: Arc<LogManager>,
        tx_id: TxID,
        num_chunks: usize,
    ) -> (SerialSyncController, UnboundedReceiver<NetworkMessage>) {
        let (network_send, network_recv) = mpsc::unbounded_channel::<NetworkMessage>();
        let ctx = Arc::new(SyncNetworkContext::new(network_send));

        let peer_id = match peer_id {
            Some(v) => v,
            _ => identity::Keypair::generate_ed25519().public().to_peer_id(),
        };

        let file_location_cache = create_file_location_cache(peer_id, vec![tx_id]);

        let controller = SerialSyncController::new(
            tx_id,
            0,
            FileSyncGoal::new_file(num_chunks as u64),
            ctx,
            Store::new(store, task_executor),
            file_location_cache,
        );

        (controller, network_recv)
    }
}
