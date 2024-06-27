#![cfg(test)]

use libp2p::gossipsub::GossipsubConfigBuilder;
use network::Enr;
use network::EnrExt;
use network::Multiaddr;
use network::Service as LibP2PService;
use network::{Libp2pEvent, NetworkConfig};
use std::sync::Weak;
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::{debug, error};
use unused_port::unused_tcp_port;

#[allow(clippy::type_complexity)]
#[allow(unused)]
pub mod behaviour;
#[allow(clippy::type_complexity)]
#[allow(unused)]
pub mod swarm;

type ReqId = usize;

use tempfile::Builder as TempBuilder;
use tokio::sync::mpsc::unbounded_channel;

#[allow(unused)]
pub struct Libp2pInstance(LibP2PService<ReqId>, exit_future::Signal);

impl std::ops::Deref for Libp2pInstance {
    type Target = LibP2PService<ReqId>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Libp2pInstance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub fn build_config(port: u16, mut boot_nodes: Vec<Enr>) -> NetworkConfig {
    let mut config = NetworkConfig::default();
    let path = TempBuilder::new()
        .prefix(&format!("libp2p_test{}", port))
        .tempdir()
        .unwrap();

    config.libp2p_port = port; // tcp port
    config.discovery_port = port; // udp port
    config.enr_tcp_port = Some(port);
    config.enr_udp_port = Some(port);
    config.enr_address = Some("127.0.0.1".parse().unwrap());
    config.boot_nodes_enr.append(&mut boot_nodes);
    config.network_dir = path.into_path();
    // Reduce gossipsub heartbeat parameters
    config.gs_config = GossipsubConfigBuilder::from(config.gs_config)
        .heartbeat_initial_delay(Duration::from_millis(500))
        .heartbeat_interval(Duration::from_millis(500))
        .build()
        .unwrap();
    config
}

pub async fn build_libp2p_instance(rt: Weak<Runtime>, boot_nodes: Vec<Enr>) -> Libp2pInstance {
    let port = unused_tcp_port().unwrap();
    let config = build_config(port, boot_nodes);
    // launch libp2p service

    let (signal, exit) = exit_future::signal();
    let (shutdown_tx, _) = futures::channel::mpsc::channel(1);
    let executor = task_executor::TaskExecutor::new(rt, exit, shutdown_tx);
    let libp2p_context = network::Context { config: &config };
    let (sender, _) = unbounded_channel();
    Libp2pInstance(
        LibP2PService::new(executor, sender, libp2p_context)
            .await
            .expect("should build libp2p instance")
            .2,
        signal,
    )
}

#[allow(dead_code)]
pub fn get_enr(node: &LibP2PService<ReqId>) -> Enr {
    node.swarm.behaviour().local_enr()
}

// Returns `n` libp2p peers in fully connected topology.
#[allow(dead_code)]
pub async fn build_full_mesh(rt: Weak<Runtime>, n: usize) -> Vec<Libp2pInstance> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(build_libp2p_instance(rt.clone(), vec![]).await);
    }
    let multiaddrs: Vec<Multiaddr> = nodes
        .iter()
        .map(|x| get_enr(x).multiaddr()[1].clone())
        .collect();

    for (i, node) in nodes.iter_mut().enumerate().take(n) {
        for (j, multiaddr) in multiaddrs.iter().enumerate().skip(i) {
            if i != j {
                match libp2p::Swarm::dial(&mut node.swarm, multiaddr.clone()) {
                    Ok(()) => debug!("Connected"),
                    Err(_) => error!("Failed to connect"),
                };
            }
        }
    }
    nodes
}

// Constructs a pair of nodes with separate loggers. The sender dials the receiver.
// This returns a (sender, receiver) pair.
#[allow(dead_code)]
pub async fn build_node_pair(rt: Weak<Runtime>) -> (Libp2pInstance, Libp2pInstance) {
    let mut sender = build_libp2p_instance(rt.clone(), vec![]).await;
    let mut receiver = build_libp2p_instance(rt, vec![]).await;

    let receiver_multiaddr = receiver.swarm.behaviour_mut().local_enr().multiaddr()[1].clone();

    // let the two nodes set up listeners
    let sender_fut = async {
        loop {
            if let Libp2pEvent::NewListenAddr(_) = sender.next_event().await {
                return;
            }
        }
    };
    let receiver_fut = async {
        loop {
            if let Libp2pEvent::NewListenAddr(_) = receiver.next_event().await {
                return;
            }
        }
    };

    let joined = futures::future::join(sender_fut, receiver_fut);

    // wait for either both nodes to listen or a timeout
    tokio::select! {
        _  = tokio::time::sleep(Duration::from_millis(500)) => {}
        _ = joined => {}
    }

    match libp2p::Swarm::dial(&mut sender.swarm, receiver_multiaddr.clone()) {
        Ok(()) => {
            debug!(address = %format!("{:?}", receiver_multiaddr), "Sender dialed receiver")
        }
        Err(_) => error!("Dialing failed"),
    };
    (sender, receiver)
}

// Returns `n` peers in a linear topology
#[allow(dead_code)]
pub async fn build_linear(rt: Weak<Runtime>, n: usize) -> Vec<Libp2pInstance> {
    let mut nodes = Vec::with_capacity(n);
    for _ in 0..n {
        nodes.push(build_libp2p_instance(rt.clone(), vec![]).await);
    }

    let multiaddrs: Vec<Multiaddr> = nodes
        .iter()
        .map(|x| get_enr(x).multiaddr()[1].clone())
        .collect();
    for i in 0..n - 1 {
        match libp2p::Swarm::dial(&mut nodes[i].swarm, multiaddrs[i + 1].clone()) {
            Ok(()) => debug!("Connected"),
            Err(_) => error!("Failed to connect"),
        };
    }
    nodes
}
