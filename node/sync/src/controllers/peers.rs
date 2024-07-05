use file_location_cache::FileLocationCache;
use network::{Multiaddr, PeerAction, PeerId};
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use shared_types::TxID;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::vec;
use storage::config::ShardConfig;

use crate::context::SyncNetworkContext;
use crate::InstantWrapper;

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerState {
    Found,
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
}

#[derive(Debug)]
struct PeerInfo {
    /// The reported/connected address of the peer.
    pub addr: Multiaddr,

    /// The current state of the peer.
    pub state: PeerState,

    pub shard_config: ShardConfig,

    /// Timestamp of the last state change.
    pub since: InstantWrapper,
}

impl PeerInfo {
    fn update_state(&mut self, new_state: PeerState) {
        self.state = new_state;
        self.since = Instant::now().into();
    }
}

#[derive(Default)]
pub struct SyncPeers {
    peers: HashMap<PeerId, PeerInfo>,
    ctx: Option<Arc<SyncNetworkContext>>,
    file_location_cache: Option<(TxID, Arc<FileLocationCache>)>,
}

impl SyncPeers {
    pub fn new(
        ctx: Arc<SyncNetworkContext>,
        tx_id: TxID,
        file_location_cache: Arc<FileLocationCache>,
    ) -> Self {
        Self {
            peers: Default::default(),
            ctx: Some(ctx),
            file_location_cache: Some((tx_id, file_location_cache)),
        }
    }

    pub fn states(&self) -> HashMap<PeerState, u64> {
        let mut states: HashMap<PeerState, u64> = HashMap::new();

        for info in self.peers.values() {
            let num = states.get(&info.state).map_or(0, |x| *x);
            states.insert(info.state, num + 1);
        }

        states
    }

    pub fn add_new_peer_with_config(
        &mut self,
        peer_id: PeerId,
        addr: Multiaddr,
        shard_config: ShardConfig,
    ) -> bool {
        if let Some(info) = self.peers.get(&peer_id) {
            if info.shard_config == shard_config {
                return false;
            }
        }

        self.peers.insert(
            peer_id,
            PeerInfo {
                addr,
                state: PeerState::Found,
                shard_config,
                since: Instant::now().into(),
            },
        );

        true
    }

    #[cfg(test)]
    pub fn add_new_peer(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        self.add_new_peer_with_config(peer_id, addr, Default::default())
    }

    pub fn update_state(
        &mut self,
        peer_id: &PeerId,
        from: PeerState,
        to: PeerState,
    ) -> Option<bool> {
        let info = self.peers.get_mut(peer_id)?;

        if info.state == from {
            info.update_state(to);
            Some(true)
        } else {
            Some(false)
        }
    }

    pub fn update_state_force(&mut self, peer_id: &PeerId, state: PeerState) -> Option<PeerState> {
        let info = self.peers.get_mut(peer_id)?;
        let old_state = info.state;
        info.state = state;
        Some(old_state)
    }

    pub fn peer_state(&self, peer_id: &PeerId) -> Option<PeerState> {
        self.peers.get(peer_id).map(|info| info.state)
    }

    pub fn shard_config(&self, peer_id: &PeerId) -> Option<ShardConfig> {
        self.peers.get(peer_id).map(|info| info.shard_config)
    }

    pub fn random_peer(&self, state: PeerState) -> Option<(PeerId, Multiaddr)> {
        self.peers
            .iter()
            .filter(|(_, info)| info.state == state)
            .map(|(peer_id, info)| (*peer_id, info.addr.clone()))
            .choose(&mut rand::thread_rng())
    }

    pub fn filter_peers(&self, state: Vec<PeerState>) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(peer_id, info)| {
                if state.contains(&info.state) {
                    Some(*peer_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn count(&self, states: &[PeerState]) -> usize {
        self.peers
            .values()
            .filter(|info| states.contains(&info.state))
            .count()
    }

    pub fn all_shards_available(&self, state: Vec<PeerState>) -> bool {
        let mut missing_shards = BTreeSet::new();
        missing_shards.insert(0);
        let mut num_shards = 1usize;
        for peer_id in &self.filter_peers(state) {
            let shard_config = self.peers.get(peer_id).unwrap().shard_config;
            match shard_config.num_shard.cmp(&num_shards) {
                Ordering::Equal => {
                    missing_shards.remove(&shard_config.shard_id);
                }
                Ordering::Less => {
                    let multi = num_shards / shard_config.num_shard;
                    for i in 0..multi {
                        let shard_id = shard_config.shard_id + i * shard_config.num_shard;
                        missing_shards.remove(&shard_id);
                    }
                }
                Ordering::Greater => {
                    let multi = shard_config.num_shard / num_shards;
                    let mut new_missing_shards = BTreeSet::new();
                    for shard_id in &missing_shards {
                        for i in 0..multi {
                            new_missing_shards.insert(*shard_id + i * num_shards);
                        }
                    }
                    new_missing_shards.remove(&shard_config.shard_id);

                    missing_shards = new_missing_shards;
                    num_shards = shard_config.num_shard;
                }
            }
        }
        trace!("all_shards_available: {} {:?}", num_shards, missing_shards);
        missing_shards.is_empty()
    }

    pub fn transition(&mut self) {
        let mut bad_peers = vec![];

        for (peer_id, info) in self.peers.iter_mut() {
            match info.state {
                PeerState::Found | PeerState::Connected => {}

                PeerState::Connecting => {
                    if info.since.elapsed() >= PEER_CONNECT_TIMEOUT {
                        info!(%peer_id, %info.addr, "Peer connection timeout");
                        bad_peers.push(*peer_id);

                        // Ban peer in case of continuous connection timeout
                        if let Some(ctx) = &self.ctx {
                            ctx.report_peer(
                                *peer_id,
                                PeerAction::LowToleranceError,
                                "Dail timeout",
                            );
                        }

                        // Remove cached file announcement if connection timeout
                        if let Some((tx_id, cache)) = &self.file_location_cache {
                            cache.remove(tx_id, peer_id);
                        }
                    }
                }

                PeerState::Disconnecting => {
                    if info.since.elapsed() >= PEER_DISCONNECT_TIMEOUT {
                        info!(%peer_id, %info.addr, "Peer disconnect timeout");
                        bad_peers.push(*peer_id);
                    }
                }

                PeerState::Disconnected => bad_peers.push(*peer_id),
            }
        }

        for peer_id in bad_peers {
            self.peers.remove(&peer_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use libp2p::identity;
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_add_new_peer() {
        let mut sync_peers: SyncPeers = Default::default();
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        assert!(sync_peers.add_new_peer(peer_id, addr.clone()));
        assert!(!sync_peers.add_new_peer(peer_id, addr));
    }

    #[test]
    fn test_update_state() {
        let mut sync_peers: SyncPeers = Default::default();
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        assert_eq!(
            sync_peers.update_state(&peer_id, PeerState::Found, PeerState::Connecting),
            None
        );
        assert_eq!(sync_peers.peer_state(&peer_id), None);

        sync_peers.add_new_peer(peer_id, addr);
        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Found));

        assert_eq!(
            sync_peers.update_state(&peer_id, PeerState::Found, PeerState::Connecting),
            Some(true)
        );
        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Connecting));

        assert_eq!(
            sync_peers.update_state(&peer_id, PeerState::Found, PeerState::Connected),
            Some(false)
        );
        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Connecting));
    }

    #[test]
    fn test_update_state_force() {
        let mut sync_peers: SyncPeers = Default::default();
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        assert_eq!(
            sync_peers.update_state_force(&peer_id, PeerState::Connecting),
            None
        );
        assert_eq!(sync_peers.peer_state(&peer_id), None);

        sync_peers.add_new_peer(peer_id, addr);

        assert_eq!(
            sync_peers.update_state_force(&peer_id, PeerState::Connecting),
            Some(PeerState::Found)
        );
        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Connecting));
    }

    #[test]
    fn test_random_peer() {
        let count = 10;
        let mut sync_peers: SyncPeers = Default::default();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        let mut peers_found = HashSet::new();
        let mut peers_connecting = HashSet::new();

        for i in 0..count {
            let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
            sync_peers.add_new_peer(peer_id, addr.clone());
            peers_found.insert(peer_id);

            assert_eq!(sync_peers.count(&[PeerState::Found]), i + 1);
            assert_eq!(sync_peers.count(&[PeerState::Connecting]), 0);
            assert_eq!(
                sync_peers.count(&[PeerState::Found, PeerState::Connecting]),
                i + 1
            );
        }

        for i in 0..count {
            let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
            sync_peers.add_new_peer(peer_id, addr.clone());
            sync_peers.update_state_force(&peer_id, PeerState::Connecting);
            peers_connecting.insert(peer_id);

            assert_eq!(sync_peers.count(&[PeerState::Found]), count);
            assert_eq!(sync_peers.count(&[PeerState::Connecting]), i + 1);
            assert_eq!(
                sync_peers.count(&[PeerState::Found, PeerState::Connecting]),
                count + i + 1
            );
        }

        // random pick
        for _ in 0..30 {
            let peer = sync_peers.random_peer(PeerState::Found).unwrap();
            assert!(peers_found.contains(&peer.0));
            assert_eq!(peer.1, addr);
            let peer = sync_peers.random_peer(PeerState::Connecting).unwrap();
            assert!(peers_connecting.contains(&peer.0));
            assert_eq!(peer.1, addr);
            assert!(sync_peers.random_peer(PeerState::Disconnected).is_none());
        }
    }

    #[test]
    fn test_transition() {
        let mut sync_peers: SyncPeers = Default::default();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id, addr.clone());

        let peer_id_connected = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id_connected, addr.clone());
        sync_peers.update_state_force(&peer_id_connected, PeerState::Connected);

        let peer_id_connecting = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id_connecting, addr.clone());
        sync_peers.update_state_force(&peer_id_connecting, PeerState::Connecting);
        sync_peers.peers.get_mut(&peer_id_connecting).unwrap().since =
            (Instant::now() - PEER_CONNECT_TIMEOUT).into();

        let peer_id_disconnecting = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id_disconnecting, addr.clone());
        sync_peers.update_state_force(&peer_id_disconnecting, PeerState::Disconnecting);
        sync_peers
            .peers
            .get_mut(&peer_id_disconnecting)
            .unwrap()
            .since = (Instant::now() - PEER_DISCONNECT_TIMEOUT).into();

        let peer_id_disconnected = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id_disconnected, addr);
        sync_peers.update_state_force(&peer_id_disconnected, PeerState::Disconnected);

        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Found));
        assert_eq!(
            sync_peers.peer_state(&peer_id_connected),
            Some(PeerState::Connected)
        );
        assert_eq!(
            sync_peers.peer_state(&peer_id_connecting),
            Some(PeerState::Connecting)
        );
        assert_eq!(
            sync_peers.peer_state(&peer_id_disconnecting),
            Some(PeerState::Disconnecting)
        );
        assert_eq!(
            sync_peers.peer_state(&peer_id_disconnected),
            Some(PeerState::Disconnected)
        );

        sync_peers.transition();

        assert_eq!(sync_peers.peer_state(&peer_id), Some(PeerState::Found));
        assert_eq!(
            sync_peers.peer_state(&peer_id_connected),
            Some(PeerState::Connected)
        );
        assert_eq!(sync_peers.peer_state(&peer_id_connecting), None);
        assert_eq!(sync_peers.peer_state(&peer_id_disconnecting), None);
        assert_eq!(sync_peers.peer_state(&peer_id_disconnected), None);
    }
}
