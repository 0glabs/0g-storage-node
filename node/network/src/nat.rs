//! This houses various NAT hole punching strategies.
//!
//! Currently supported strategies:
//! - UPnP

use crate::{NetworkConfig, NetworkMessage};
use if_addrs::get_if_addrs;
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use tokio::sync::mpsc;

/// Configuration required to construct the UPnP port mappings.
pub struct UPnPConfig {
    /// The local tcp port.
    tcp_port: u16,
    /// The local udp port.
    udp_port: u16,
    /// Whether discovery is enabled or not.
    disable_discovery: bool,
}

impl UPnPConfig {
    pub fn from_config(config: &NetworkConfig) -> Option<Self> {
        if config.enr_tcp_port.is_some()
            || config.enr_udp_port.is_some()
            || config.enr_address.is_some()
        {
            None
        } else {
            Some(UPnPConfig {
                tcp_port: config.libp2p_port,
                udp_port: config.discovery_port,
                disable_discovery: config.disable_discovery,
            })
        }
    }
}

/// Attempts to construct external port mappings with UPnP.
pub fn construct_upnp_mappings(
    config: UPnPConfig,
    network_send: mpsc::UnboundedSender<NetworkMessage>,
) {
    info!("UPnP Attempting to initialise routes");
    match igd::search_gateway(Default::default()) {
        Err(e) => info!(error = %e, "UPnP not available"),
        Ok(gateway) => {
            // Need to find the local listening address matched with the router subnet
            let interfaces = match get_if_addrs() {
                Ok(v) => v,
                Err(e) => {
                    info!(error = %e, "UPnP failed to get local interfaces");
                    return;
                }
            };
            let local_ip = interfaces.iter().find_map(|interface| {
                // Just use the first IP of the first interface that is not a loopback and not an
                // ipv6 address.
                if !interface.is_loopback() {
                    interface.ip().is_ipv4().then(|| interface.ip())
                } else {
                    None
                }
            });

            let local_ip = match local_ip {
                None => {
                    info!("UPnP failed to find local IP address");
                    return;
                }
                Some(v) => v,
            };

            debug!(ip = %local_ip, "UPnP Local IP Discovered");

            match local_ip {
                IpAddr::V4(address) => {
                    let libp2p_socket = SocketAddrV4::new(address, config.tcp_port);
                    let external_ip = gateway.get_external_ip();
                    // We add specific port mappings rather than getting the router to arbitrary assign
                    // one.
                    // I've found this to be more reliable. If multiple users are behind a single
                    // router, they should ideally try to set different port numbers.
                    let tcp_socket = add_port_mapping(
                        &gateway,
                        igd::PortMappingProtocol::TCP,
                        libp2p_socket,
                        "tcp",
                    )
                    .and_then(|_| {
                        let external_socket = external_ip
                            .as_ref()
                            .map(|ip| SocketAddr::new((*ip).into(), config.tcp_port))
                            .map_err(|_| ());
                        info!(
                            external_socket = format!(
                                "{}:{}",
                                external_socket
                                    .as_ref()
                                    .map(|ip| ip.to_string())
                                    .unwrap_or_else(|_| "".into()),
                                config.tcp_port
                            )
                        );
                        external_socket
                    })
                    .ok();

                    let udp_socket = if !config.disable_discovery {
                        let discovery_socket = SocketAddrV4::new(address, config.udp_port);
                        add_port_mapping(
                            &gateway,
                            igd::PortMappingProtocol::UDP,
                            discovery_socket,
                            "udp",
                        )
                        .and_then(|_| {
                            let external_socket = external_ip
                                .map(|ip| SocketAddr::new(ip.into(), config.udp_port))
                                .map_err(|_| ());
                            info!(
                                external_socket = format!(
                                    "{}:{}",
                                    external_socket
                                        .as_ref()
                                        .map(|ip| ip.to_string())
                                        .unwrap_or_else(|_| "".into()),
                                    config.udp_port
                                ),
                                "UPnP UDP route established"
                            );
                            external_socket
                        })
                        .ok()
                    } else {
                        None
                    };

                    // report any updates to the network service.
                    network_send
                        .send(NetworkMessage::UPnPMappingEstablished {
                            tcp_socket,
                            udp_socket,
                        })
                        .unwrap_or_else(
                            |e| debug!(error = %e, "Could not send message to the network service"),
                        );
                }
                _ => debug!("UPnP no routes constructed. IPv6 not supported"),
            }
        }
    };
}

/// Sets up a port mapping for a protocol returning the mapped port if successful.
fn add_port_mapping(
    gateway: &igd::Gateway,
    protocol: igd::PortMappingProtocol,
    socket: SocketAddrV4,
    protocol_string: &'static str,
) -> Result<(), ()> {
    // We add specific port mappings rather than getting the router to arbitrary assign
    // one.
    // I've found this to be more reliable. If multiple users are behind a single
    // router, they should ideally try to set different port numbers.
    let mapping_string = &format!("lighthouse-{}", protocol_string);
    for _ in 0..2 {
        match gateway.add_port(protocol, socket.port(), socket, 0, mapping_string) {
            Err(e) => {
                match e {
                    igd::AddPortError::PortInUse => {
                        // Try and remove and re-create
                        debug!(
                            protocol = protocol_string,
                            port = socket.port(),
                            "UPnP port in use, attempting to remap"
                        );
                        match gateway.remove_port(protocol, socket.port()) {
                            Ok(()) => {
                                debug!(
                                    protocol = protocol_string,
                                    port = socket.port(),
                                    "UPnP Removed port mapping"
                                )
                            }
                            Err(e) => {
                                debug!(protocol = protocol_string, port = socket.port(), error = %e, "UPnP Port remove failure");
                                return Err(());
                            }
                        }
                    }
                    e => {
                        info!(error = %e, "UPnP TCP route not set");
                        return Err(());
                    }
                }
            }
            Ok(_) => {
                return Ok(());
            }
        }
    }
    Err(())
}

/// Removes the specified TCP and UDP port mappings.
pub fn remove_mappings(tcp_port: Option<u16>, udp_port: Option<u16>) {
    if tcp_port.is_some() || udp_port.is_some() {
        debug!("Removing UPnP port mappings");
        match igd::search_gateway(Default::default()) {
            Ok(gateway) => {
                if let Some(tcp_port) = tcp_port {
                    match gateway.remove_port(igd::PortMappingProtocol::TCP, tcp_port) {
                        Ok(()) => debug!(port = tcp_port, "UPnP Removed TCP port mapping"),
                        Err(e) => {
                            debug!(port = tcp_port, error = %e, "UPnP Failed to remove TCP port mapping")
                        }
                    }
                }
                if let Some(udp_port) = udp_port {
                    match gateway.remove_port(igd::PortMappingProtocol::UDP, udp_port) {
                        Ok(()) => debug!(port = tcp_port, "UPnP Removed UDP port mapping"),
                        Err(e) => {
                            debug!(port = tcp_port, error = %e, "UPnP Failed to remove UDP port mapping")
                        }
                    }
                }
            }
            Err(e) => debug!(error = %e, "UPnP failed to remove mappings"),
        }
    }
}
