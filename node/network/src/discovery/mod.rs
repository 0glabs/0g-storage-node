//! The discovery sub-behaviour of Lighthouse.
//!
//! This module creates a libp2p dummy-behaviour built around the discv5 protocol. It handles
//! queries and manages access to the discovery routing table.

pub(crate) mod enr;
pub mod enr_ext;

use crate::metrics;
use crate::{error, Enr, NetworkConfig, NetworkGlobals};
use discv5::{enr::NodeId, Discv5, Discv5Event};
pub use enr::{
    build_enr, create_enr_builder_from_config, load_enr_from_disk, use_or_load_enr, CombinedKey,
};
pub use enr_ext::{peer_id_to_node_id, CombinedKeyExt, EnrExt};
pub use libp2p::core::identity::{Keypair, PublicKey};

use futures::prelude::*;
use futures::stream::FuturesUnordered;
pub use libp2p::{
    core::{connection::ConnectionId, ConnectedPoint, Multiaddr, PeerId},
    swarm::{
        handler::ConnectionHandler, DialError, NetworkBehaviour,
        NetworkBehaviourAction as NBAction, NotifyHandler, PollParameters, SubstreamProtocol,
    },
};
use lru::LruCache;
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::sync::mpsc;

/// Local ENR storage filename.
pub const ENR_FILENAME: &str = "enr.dat";
/// The number of closest peers to search for when doing a regular peer search.
///
/// We could reduce this constant to speed up queries however at the cost of security. It will
/// make it easier to peers to eclipse this node. Kademlia suggests a value of 16.
pub const FIND_NODE_QUERY_CLOSEST_PEERS: usize = 16;
/// The threshold for updating `min_ttl` on a connected peer.

/// The events emitted by polling discovery.
pub enum DiscoveryEvent {
    /// A query has completed. This result contains a mapping of discovered peer IDs to the `min_ttl`
    /// of the peer if it is specified.
    QueryResult(HashMap<PeerId, Option<Instant>>),
    /// This indicates that our local UDP socketaddr has been updated and we should inform libp2p.
    SocketUpdated(SocketAddr),
}

#[derive(Debug, Clone, PartialEq)]
enum QueryType {
    /// We are searching for more peers without ENR or time constraints.
    FindPeers,
}

/// The result of a query.
struct QueryResult {
    query_type: QueryType,
    result: Result<Vec<Enr>, discv5::QueryError>,
}

// Awaiting the event stream future
enum EventStream {
    /// Awaiting an event stream to be generated. This is required due to the poll nature of
    /// `Discovery`
    Awaiting(
        Pin<
            Box<
                dyn Future<Output = Result<mpsc::Receiver<Discv5Event>, discv5::Discv5Error>>
                    + Send,
            >,
        >,
    ),
    /// The future has completed.
    Present(mpsc::Receiver<Discv5Event>),
    // The future has failed or discv5 has been disabled. There are no events from discv5.
    InActive,
}

/// The main discovery service. This can be disabled via CLI arguements. When disabled the
/// underlying processes are not started, but this struct still maintains our current ENR.
pub struct Discovery {
    /// A collection of seen live ENRs for quick lookup and to map peer-id's to ENRs.
    cached_enrs: LruCache<PeerId, Enr>,

    /// The directory where the ENR is stored.
    enr_dir: String,

    /// The handle for the underlying discv5 Server.
    ///
    /// This is behind a Reference counter to allow for futures to be spawned and polled with a
    /// static lifetime.
    discv5: Discv5,

    /// A collection of network constants that can be read from other threads.
    network_globals: Arc<NetworkGlobals>,

    /// Indicates if we are actively searching for peers. We only allow a single FindPeers query at
    /// a time, regardless of the query concurrency.
    find_peer_active: bool,

    /// Active discovery queries.
    active_queries: FuturesUnordered<std::pin::Pin<Box<dyn Future<Output = QueryResult> + Send>>>,

    /// The discv5 event stream.
    event_stream: EventStream,

    /// Indicates if the discovery service has been started. When the service is disabled, this is
    /// always false.
    pub started: bool,
}

impl Discovery {
    /// NOTE: Creating discovery requires running within a tokio execution environment.
    pub async fn new(
        local_key: &Keypair,
        config: &NetworkConfig,
        network_globals: Arc<NetworkGlobals>,
    ) -> error::Result<Self> {
        let enr_dir = match config.network_dir.to_str() {
            Some(path) => String::from(path),
            None => String::from(""),
        };

        let local_enr = network_globals.local_enr.read().clone();

        info!(
            enr = %local_enr.to_base64(),
            seq = %local_enr.seq(),
            id = %local_enr.node_id(),
            ip = ?local_enr.ip(),
            udp = ?local_enr.udp(),
            tcp = ?local_enr.tcp(),
            udp4_socket = ?local_enr.udp_socket(),
            "ENR Initialised",
        );

        let listen_socket = SocketAddr::new(config.listen_address, config.discovery_port);

        // convert the keypair into an ENR key
        let enr_key: CombinedKey = CombinedKey::from_libp2p(local_key)?;

        let mut discv5 = Discv5::new(local_enr, enr_key, config.discv5_config.clone())
            .map_err(|e| format!("Discv5 service failed. Error: {:?}", e))?;

        // Add bootnodes to routing table
        for bootnode_enr in config.boot_nodes_enr.clone() {
            debug!(
                node_id = %bootnode_enr.node_id(),
                peer_id = %bootnode_enr.peer_id(),
                ip = ?bootnode_enr.ip(),
                udp = ?bootnode_enr.udp(),
                tcp = ?bootnode_enr.tcp(),
                "Adding node to routing table",
            );
            let repr = bootnode_enr.to_string();
            let _ = discv5.add_enr(bootnode_enr).map_err(|e| {
                error!(
                    addr = %repr,
                    error = %e.to_string(),
                    "Could not add peer to the local routing table",
                )
            });
        }

        // Start the discv5 service and obtain an event stream
        let event_stream = if !config.disable_discovery {
            discv5
                .start(listen_socket)
                .map_err(|e| e.to_string())
                .await?;
            debug!("Discovery service started");
            EventStream::Awaiting(Box::pin(discv5.event_stream()))
        } else {
            EventStream::InActive
        };

        if !config.boot_nodes_multiaddr.is_empty() {
            info!("Contacting Multiaddr boot-nodes for their ENR");
        }

        // get futures for requesting the Enrs associated to these multiaddr and wait for their
        // completion
        let mut fut_coll = config
            .boot_nodes_multiaddr
            .iter()
            .map(|addr| addr.to_string())
            // request the ENR for this multiaddr and keep the original for logging
            .map(|addr| {
                futures::future::join(
                    discv5.request_enr(addr.clone()),
                    futures::future::ready(addr),
                )
            })
            .collect::<FuturesUnordered<_>>();

        while let Some((result, original_addr)) = fut_coll.next().await {
            match result {
                Ok(enr) => {
                    debug!(
                        node_id = %enr.node_id(),
                        peer_id = %enr.peer_id(),
                        ip = ?enr.ip(),
                        udp = ?enr.udp(),
                        tcp = ?enr.tcp(),
                        "Adding node to routing table",
                    );
                    let _ = discv5.add_enr(enr).map_err(|e| {
                        error!(
                            addr = %original_addr.to_string(),
                            error = %e.to_string(),
                            "Could not add peer to the local routing table",
                        )
                    });
                }
                Err(e) => {
                    error!(
                        multiaddr = %original_addr.to_string(),
                        error = %e.to_string(),
                        "Error getting mapping to ENR",
                    )
                }
            }
        }

        Ok(Self {
            cached_enrs: LruCache::new(50),
            network_globals,
            find_peer_active: false,
            active_queries: FuturesUnordered::new(),
            discv5,
            event_stream,
            started: !config.disable_discovery,
            enr_dir,
        })
    }

    /// Return the nodes local ENR.
    pub fn local_enr(&self) -> Enr {
        self.discv5.local_enr()
    }

    /// Return the cached enrs.
    pub fn cached_enrs(&self) -> impl Iterator<Item = (&PeerId, &Enr)> {
        self.cached_enrs.iter()
    }

    /// Removes a cached ENR from the list.
    pub fn remove_cached_enr(&mut self, peer_id: &PeerId) -> Option<Enr> {
        self.cached_enrs.pop(peer_id)
    }

    /// This adds a new `FindPeers` query to the queue if one doesn't already exist.
    /// The `target_peers` parameter informs discovery to end the query once the target is found.
    /// The maximum this can be is 16.
    pub fn discover_peers(&mut self, target_peers: usize) {
        // If the discv5 service isn't running or we are in the process of a query, don't bother queuing a new one.
        if !self.started || self.find_peer_active {
            return;
        }
        // Immediately start a FindNode query
        let target_peers = std::cmp::min(FIND_NODE_QUERY_CLOSEST_PEERS, target_peers);
        debug!(%target_peers, "Starting a peer discovery request");
        self.find_peer_active = true;
        self.start_query(QueryType::FindPeers, target_peers);
    }

    /// Add an ENR to the routing table of the discovery mechanism.
    pub fn add_enr(&mut self, enr: Enr) {
        // add the enr to seen caches
        self.cached_enrs.put(enr.peer_id(), enr.clone());

        if let Err(e) = self.discv5.add_enr(enr) {
            debug!(
                error = %e,
                "Could not add peer to the local routing table",
            )
        }
    }

    /// Returns an iterator over all enr entries in the DHT.
    pub fn table_entries_enr(&mut self) -> Vec<Enr> {
        self.discv5.table_entries_enr()
    }

    /// Returns the ENR of a known peer if it exists.
    pub fn enr_of_peer(&mut self, peer_id: &PeerId) -> Option<Enr> {
        // first search the local cache
        if let Some(enr) = self.cached_enrs.get(peer_id) {
            return Some(enr.clone());
        }
        // not in the local cache, look in the routing table
        if let Ok(node_id) = enr_ext::peer_id_to_node_id(peer_id) {
            self.discv5.find_enr(&node_id)
        } else {
            None
        }
    }

    /// Updates the local ENR TCP port.
    /// There currently isn't a case to update the address here. We opt for discovery to
    /// automatically update the external address.
    ///
    /// If the external address needs to be modified, use `update_enr_udp_socket.
    pub fn update_enr_tcp_port(&mut self, port: u16) -> Result<(), String> {
        self.discv5
            .enr_insert("tcp", &port.to_be_bytes())
            .map_err(|e| format!("{:?}", e))?;

        // replace the global version
        *self.network_globals.local_enr.write() = self.discv5.local_enr();
        // persist modified enr to disk
        enr::save_enr_to_disk(Path::new(&self.enr_dir), &self.local_enr());
        Ok(())
    }

    /// Updates the local ENR UDP socket.
    ///
    /// This is with caution. Discovery should automatically maintain this. This should only be
    /// used when automatic discovery is disabled.
    pub fn update_enr_udp_socket(&mut self, socket_addr: SocketAddr) -> Result<(), String> {
        match socket_addr {
            SocketAddr::V4(socket) => {
                self.discv5
                    .enr_insert("ip", &socket.ip().octets())
                    .map_err(|e| format!("{:?}", e))?;
                self.discv5
                    .enr_insert("udp", &socket.port().to_be_bytes())
                    .map_err(|e| format!("{:?}", e))?;
            }
            SocketAddr::V6(socket) => {
                self.discv5
                    .enr_insert("ip6", &socket.ip().octets())
                    .map_err(|e| format!("{:?}", e))?;
                self.discv5
                    .enr_insert("udp6", &socket.port().to_be_bytes())
                    .map_err(|e| format!("{:?}", e))?;
            }
        }

        // replace the global version
        *self.network_globals.local_enr.write() = self.discv5.local_enr();
        // persist modified enr to disk
        enr::save_enr_to_disk(Path::new(&self.enr_dir), &self.local_enr());
        Ok(())
    }

    // Bans a peer and it's associated seen IP addresses.
    pub fn ban_peer(&mut self, peer_id: &PeerId, ip_addresses: Vec<IpAddr>) {
        // first try and convert the peer_id to a node_id.
        if let Ok(node_id) = peer_id_to_node_id(peer_id) {
            // If we could convert this peer id, remove it from the DHT and ban it from discovery.
            self.discv5.ban_node(&node_id, None);
            // Remove the node from the routing table.
            self.discv5.remove_node(&node_id);
        }

        for ip_address in ip_addresses {
            self.discv5.ban_ip(ip_address, None);
        }
    }

    /// Unbans the peer in discovery.
    pub fn unban_peer(&mut self, peer_id: &PeerId, ip_addresses: Vec<IpAddr>) {
        // first try and convert the peer_id to a node_id.
        if let Ok(node_id) = peer_id_to_node_id(peer_id) {
            self.discv5.ban_node_remove(&node_id);
        }

        for ip_address in ip_addresses {
            self.discv5.ban_ip_remove(&ip_address);
        }
    }

    ///  Marks node as disconnected in the DHT, freeing up space for other nodes, this also removes
    ///  nodes from the cached ENR list.
    pub fn disconnect_peer(&mut self, peer_id: &PeerId) {
        if let Ok(node_id) = peer_id_to_node_id(peer_id) {
            self.discv5.disconnect_node(&node_id);
        }
        // Remove the peer from the cached list, to prevent redialing disconnected
        // peers.
        self.cached_enrs.pop(peer_id);
    }

    /* Internal Functions */

    /// Search for a specified number of new peers using the underlying discovery mechanism.
    ///
    /// This can optionally search for peers for a given predicate. Regardless of the predicate
    /// given, this will only search for peers on the same enr_fork_id as specified in the local
    /// ENR.
    fn start_query(&mut self, query: QueryType, target_peers: usize) {
        // Generate a random target node id.
        let random_node = NodeId::random();

        // Build the future
        let query_future = self
            .discv5
            .find_node_predicate(random_node, Box::new(|_| true), target_peers)
            .map(|v| QueryResult {
                query_type: query,
                result: v,
            });

        // Add the future to active queries, to be executed.
        self.active_queries.push(Box::pin(query_future));
    }

    /// Process the completed QueryResult returned from discv5.
    fn process_completed_queries(
        &mut self,
        query: QueryResult,
    ) -> Option<HashMap<PeerId, Option<Instant>>> {
        match query.query_type {
            QueryType::FindPeers => {
                self.find_peer_active = false;
                match query.result {
                    Ok(r) if r.is_empty() => {
                        debug!("Discovery query yielded no results.");
                    }
                    Ok(r) => {
                        debug!(peers_found = r.len(), "Discovery query completed");
                        let mut results: HashMap<_, Option<Instant>> = HashMap::new();
                        r.iter().for_each(|enr| {
                            // cache the found ENR's
                            self.cached_enrs.put(enr.peer_id(), enr.clone());
                            results.insert(enr.peer_id(), None);
                        });
                        return Some(results);
                    }
                    Err(e) => {
                        warn!(error = %e, "Discovery query failed");
                    }
                }
            }
        }

        None
    }

    /// Drives the queries returning any results from completed queries.
    fn poll_queries(&mut self, cx: &mut Context) -> Option<HashMap<PeerId, Option<Instant>>> {
        while let Poll::Ready(Some(query_result)) = self.active_queries.poll_next_unpin(cx) {
            let result = self.process_completed_queries(query_result);
            if result.is_some() {
                return result;
            }
        }
        None
    }
}

/* NetworkBehaviour Implementation */

impl NetworkBehaviour for Discovery {
    // Discovery is not a real NetworkBehaviour...
    type ConnectionHandler = libp2p::swarm::handler::DummyConnectionHandler;
    type OutEvent = DiscoveryEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        libp2p::swarm::handler::DummyConnectionHandler::default()
    }

    // Handles the libp2p request to obtain multiaddrs for peer_id's in order to dial them.
    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        if let Some(enr) = self.enr_of_peer(peer_id) {
            // ENR's may have multiple Multiaddrs. The multi-addr associated with the UDP
            // port is removed, which is assumed to be associated with the discv5 protocol (and
            // therefore irrelevant for other libp2p components).
            enr.multiaddr_tcp()
        } else {
            // PeerId is not known
            Vec::new()
        }
    }

    fn inject_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        _: <Self::ConnectionHandler as ConnectionHandler>::OutEvent,
    ) {
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        _handler: Self::ConnectionHandler,
        error: &DialError,
    ) {
        if let Some(peer_id) = peer_id {
            match error {
                DialError::Banned
                | DialError::LocalPeerId
                | DialError::InvalidPeerId(_)
                | DialError::ConnectionIo(_)
                | DialError::NoAddresses
                | DialError::Transport(_)
                | DialError::WrongPeerId { .. } => {
                    // set peer as disconnected in discovery DHT
                    debug!(peer_id = %peer_id, "Marking peer disconnected in DHT");
                    self.disconnect_peer(&peer_id);
                }
                DialError::ConnectionLimit(_)
                | DialError::DialPeerConditionFalse(_)
                | DialError::Aborted => {}
            }
        }
    }

    // Main execution loop to drive the behaviour
    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<NBAction<Self::OutEvent, Self::ConnectionHandler>> {
        if !self.started {
            return Poll::Pending;
        }

        // Drive the queries and return any results from completed queries
        if let Some(results) = self.poll_queries(cx) {
            // return the result to the peer manager
            return Poll::Ready(NBAction::GenerateEvent(DiscoveryEvent::QueryResult(
                results,
            )));
        }

        // Process the server event stream
        match self.event_stream {
            EventStream::Awaiting(ref mut fut) => {
                // Still awaiting the event stream, poll it
                if let Poll::Ready(event_stream) = fut.poll_unpin(cx) {
                    match event_stream {
                        Ok(stream) => {
                            debug!("Discv5 event stream ready");
                            self.event_stream = EventStream::Present(stream);
                        }
                        Err(e) => {
                            error!(error = %e, "Discv5 event stream failed");
                            self.event_stream = EventStream::InActive;
                        }
                    }
                }
            }
            EventStream::InActive => {} // ignore checking the stream
            EventStream::Present(ref mut stream) => {
                while let Poll::Ready(Some(event)) = stream.poll_recv(cx) {
                    match event {
                        // We filter out unwanted discv5 events here and only propagate useful results to
                        // the peer manager.
                        Discv5Event::Discovered(_enr) => {
                            // Peers that get discovered during a query but are not contactable or
                            // don't match a predicate can end up here. For debugging purposes we
                            // log these to see if we are unnecessarily dropping discovered peers
                            /*
                            if enr.eth2() == self.local_enr().eth2() {
                                trace!(self.log, "Peer found in process of query"; "peer_id" => format!("{}", enr.peer_id()), "tcp_socket" => enr.tcp_socket());
                            } else {
                            // this is temporary warning for debugging the DHT
                            warn!(self.log, "Found peer during discovery not on correct fork"; "peer_id" => format!("{}", enr.peer_id()), "tcp_socket" => enr.tcp_socket());
                            }
                            */
                        }
                        Discv5Event::SocketUpdated(socket) => {
                            info!(ip = %socket.ip(), udp_port = %socket.port(), "Address updated");
                            metrics::inc_counter(&metrics::ADDRESS_UPDATE_COUNT);
                            metrics::check_nat();
                            // Discv5 will have updated our local ENR. We save the updated version
                            // to disk.
                            let enr = self.discv5.local_enr();
                            enr::save_enr_to_disk(Path::new(&self.enr_dir), &enr);
                            // update  network globals
                            *self.network_globals.local_enr.write() = enr;
                            return Poll::Ready(NBAction::GenerateEvent(
                                DiscoveryEvent::SocketUpdated(socket),
                            ));
                        }
                        Discv5Event::EnrAdded { .. }
                        | Discv5Event::TalkRequest(_)
                        | Discv5Event::NodeInserted { .. } => {} // Ignore all other discv5 server events
                    }
                }
            }
        }
        Poll::Pending
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use enr::EnrBuilder;
//     use unused_port::unused_udp_port;

//     async fn build_discovery() -> Discovery {
//         let keypair = libp2p::identity::Keypair::generate_secp256k1();
//         let config = NetworkConfig {
//             discovery_port: unused_udp_port().unwrap(),
//             ..Default::default()
//         };

//         let enr_key: CombinedKey = CombinedKey::from_libp2p(&keypair).unwrap();
//         let enr: Enr = build_enr(&enr_key, &config).unwrap();

//         let globals = NetworkGlobals::new(
//             enr,
//             9000,
//             9000,
//             vec![],
//         );

//         Discovery::new(&keypair, &config, Arc::new(globals))
//             .await
//             .unwrap()
//     }

//     fn make_enr(subnet_ids: Vec<usize>) -> Enr {
//         let mut builder = EnrBuilder::new("v4");
//         let keypair = libp2p::identity::Keypair::generate_secp256k1();
//         let enr_key: CombinedKey = CombinedKey::from_libp2p(&keypair).unwrap();
//         builder.build(&enr_key).unwrap()
//     }
// }
