use network::{Multiaddr, PeerId};
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerState {
    Found,
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
}

struct PeerInfo {
    /// The reported/connected address of the peer.
    pub addr: Multiaddr,

    /// The current state of the peer.
    pub state: PeerState,

    /// Timestamp of the last state change.
    pub since: Instant,
}

impl PeerInfo {
    fn update_state(&mut self, new_state: PeerState) {
        self.state = new_state;
        self.since = Instant::now();
    }
}

#[derive(Default)]
pub struct SyncPeers {
    peers: HashMap<PeerId, PeerInfo>,
}

impl SyncPeers {
    pub fn add_new_peer(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        if self.peers.contains_key(&peer_id) {
            return false;
        }

        self.peers.insert(
            peer_id,
            PeerInfo {
                addr,
                state: PeerState::Found,
                since: Instant::now(),
            },
        );

        true
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

    pub fn random_peer(&self, state: PeerState) -> Option<(PeerId, Multiaddr)> {
        self.peers
            .iter()
            .filter(|(_, info)| info.state == state)
            .map(|(peer_id, info)| (*peer_id, info.addr.clone()))
            .choose(&mut rand::thread_rng())
    }

    pub fn count(&self, states: &[PeerState]) -> usize {
        self.peers
            .values()
            .filter(|info| states.contains(&info.state))
            .count()
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
            Instant::now() - PEER_CONNECT_TIMEOUT;

        let peer_id_disconnecting = identity::Keypair::generate_ed25519().public().to_peer_id();
        sync_peers.add_new_peer(peer_id_disconnecting, addr.clone());
        sync_peers.update_state_force(&peer_id_disconnecting, PeerState::Disconnecting);
        sync_peers
            .peers
            .get_mut(&peer_id_disconnecting)
            .unwrap()
            .since = Instant::now() - PEER_DISCONNECT_TIMEOUT;

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
