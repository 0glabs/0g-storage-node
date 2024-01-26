#![allow(dead_code)]

mod builder;
mod environment;

use network::{Enr, Multiaddr, NetworkGlobals};
use std::sync::Arc;

pub use builder::ClientBuilder;
pub use environment::{EnvironmentBuilder, RuntimeContext};

/// The core Zgs client.
///
/// Holds references to running services, cleanly shutting them down when dropped.
pub struct Client {
    network_globals: Option<Arc<NetworkGlobals>>,
}

impl Client {
    /// Returns the port of the client's libp2p stack, if it was started.
    pub fn libp2p_listen_port(&self) -> Option<u16> {
        self.network_globals.as_ref().map(|n| n.listen_port_tcp())
    }

    /// Returns the list of libp2p addresses the client is listening to.
    pub fn libp2p_listen_addresses(&self) -> Option<Vec<Multiaddr>> {
        self.network_globals.as_ref().map(|n| n.listen_multiaddrs())
    }

    /// Returns the local libp2p ENR of this node, for network discovery.
    pub fn enr(&self) -> Option<Enr> {
        self.network_globals.as_ref().map(|n| n.local_enr())
    }
}
