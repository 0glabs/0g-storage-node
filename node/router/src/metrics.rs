use std::sync::Arc;

use metrics::{register_meter, Histogram, Meter, Sample};

lazy_static::lazy_static! {
    // service
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE: Arc<dyn Meter> = register_meter("router_service_route_network_message");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_REQUEST: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_request");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_RESPONSE: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_response");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_ERROR_RESPONSE: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_error_response");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_PUBLISH: Arc<dyn Meter> = register_meter("router_service_route_network_message_publish");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_REPORT_PEER: Arc<dyn Meter> = register_meter("router_service_route_network_message_report_peer");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_GOODBYE_PEER: Arc<dyn Meter> = register_meter("router_service_route_network_message_goodbye_peer");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER: Arc<dyn Meter> = register_meter("router_service_route_network_message_dail_peer");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_ALREADY: Arc<dyn Meter> = register_meter("router_service_route_network_message_dail_peer_already");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_NEW_OK: Arc<dyn Meter> = register_meter("router_service_route_network_message_dail_peer_new_ok");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_NEW_FAIL: Arc<dyn Meter> = register_meter("router_service_route_network_message_dail_peer_new_fail");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_ANNOUNCE_LOCAL_FILE: Arc<dyn Meter> = register_meter("router_service_route_network_message_announce_local_file");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_UPNP: Arc<dyn Meter> = register_meter("router_service_route_network_message_upnp");

    pub static ref SERVICE_EXPIRED_PEERS: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers", 1024);
    pub static ref SERVICE_EXPIRED_PEERS_DISCONNECT_OK: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers_disconnect_ok", 1024);
    pub static ref SERVICE_EXPIRED_PEERS_DISCONNECT_FAIL: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers_disconnect_fail", 1024);

    // libp2p_event_handler
    pub static ref LIBP2P_SEND_STATUS: Arc<dyn Meter> = register_meter("router_libp2p_send_status");

    pub static ref LIBP2P_HANDLE_PEER_CONNECTED_OUTGOING: Arc<dyn Meter> = register_meter("router_libp2p_handle_peer_connected_outgoing");
    pub static ref LIBP2P_HANDLE_PEER_CONNECTED_INCOMING: Arc<dyn Meter> = register_meter("router_libp2p_handle_peer_connected_incoming");
    pub static ref LIBP2P_HANDLE_PEER_DISCONNECTED: Arc<dyn Meter> = register_meter("router_libp2p_handle_peer_disconnected");
    pub static ref LIBP2P_HANDLE_REQUEST_STATUS: Arc<dyn Meter> = register_meter("router_libp2p_handle_request_status");
    pub static ref LIBP2P_HANDLE_REQUEST_GET_CHUNKS: Arc<dyn Meter> = register_meter("router_libp2p_handle_request_get_chunks");
    pub static ref LIBP2P_HANDLE_RESPONSE_STATUS: Arc<dyn Meter> = register_meter("router_libp2p_handle_response_status");
    pub static ref LIBP2P_HANDLE_RESPONSE_GET_CHUNKS: Arc<dyn Meter> = register_meter("router_libp2p_handle_response_get_chunks");
    pub static ref LIBP2P_HANDLE_RESPONSE_ERROR: Arc<dyn Meter> = register_meter("router_libp2p_handle_response_error");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE: Arc<dyn Meter> = register_meter("router_libp2p_handle_pubsub_find_file");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_CHUNKS: Arc<dyn Meter> = register_meter("router_libp2p_handle_pubsub_find_chunks");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_FILE: Arc<dyn Meter> = register_meter("router_libp2p_handle_pubsub_announce_file");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_CHUNKS: Arc<dyn Meter> = register_meter("router_libp2p_handle_pubsub_announce_chunks");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_SHARD: Arc<dyn Meter> = register_meter("router_libp2p_handle_pubsub_announce_shard");

    pub static ref LIBP2P_HANDLE_PUBSUB_LATENCY_FIND_FILE: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_libp2p_handle_pubsub_latency_find_file", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_LATENCY_FIND_CHUNKS: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_libp2p_handle_pubsub_latency_find_chunks", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_LATENCY_ANNOUNCE_FILE: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_libp2p_handle_pubsub_latency_announce_file", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_LATENCY_ANNOUNCE_CHUNKS: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_libp2p_handle_pubsub_latency_announce_chunks", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_LATENCY_ANNOUNCE_SHARD: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_libp2p_handle_pubsub_latency_announce_shard", 1024);

    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip");
    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP_UNSEEN: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip_unseen");
    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP_MISMATCH: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip_mismatch");
}
