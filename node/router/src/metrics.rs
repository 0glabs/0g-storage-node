use std::sync::Arc;

use metrics::{register_meter, register_meter_with_group, Histogram, Meter, Sample};

lazy_static::lazy_static! {
    // service
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE: Arc<dyn Meter> = register_meter("router_service_route_network_message");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_REQUEST: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_request");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_RESPONSE: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_response");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_SEND_ERROR_RESPONSE: Arc<dyn Meter> = register_meter("router_service_route_network_message_send_error_response");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_PUBLISH: Arc<dyn Meter> = register_meter("router_service_route_network_message_publish");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_REPORT_PEER: Arc<dyn Meter> = register_meter("router_service_route_network_message_report_peer");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_GOODBYE_PEER: Arc<dyn Meter> = register_meter("router_service_route_network_message_goodbye_peer");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER: Arc<dyn Meter> = register_meter_with_group("router_service_route_network_message_dail_peer", "all");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_ALREADY: Arc<dyn Meter> = register_meter_with_group("router_service_route_network_message_dail_peer", "already");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_NEW_OK: Arc<dyn Meter> = register_meter_with_group("router_service_route_network_message_dail_peer", "ok");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_DAIL_PEER_NEW_FAIL: Arc<dyn Meter> = register_meter_with_group("router_service_route_network_message_dail_peer", "fail");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_ANNOUNCE_LOCAL_FILE: Arc<dyn Meter> = register_meter("router_service_route_network_message_announce_local_file");
    pub static ref SERVICE_ROUTE_NETWORK_MESSAGE_UPNP: Arc<dyn Meter> = register_meter("router_service_route_network_message_upnp");

    pub static ref SERVICE_EXPIRED_PEERS: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers", 1024);
    pub static ref SERVICE_EXPIRED_PEERS_DISCONNECT_OK: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers_disconnect_ok", 1024);
    pub static ref SERVICE_EXPIRED_PEERS_DISCONNECT_FAIL: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_service_expired_peers_disconnect_fail", 1024);

    // libp2p_event_handler

    // libp2p_event_handler: peer connection
    pub static ref LIBP2P_HANDLE_PEER_CONNECTED_OUTGOING: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_peer_connected", "outgoing");
    pub static ref LIBP2P_HANDLE_PEER_CONNECTED_INCOMING: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_peer_connected", "incoming");
    pub static ref LIBP2P_HANDLE_PEER_DISCONNECTED: Arc<dyn Meter> = register_meter("router_libp2p_handle_peer_disconnected");

    // libp2p_event_handler: status
    pub static ref LIBP2P_SEND_STATUS: Arc<dyn Meter> = register_meter("router_libp2p_send_status");
    pub static ref LIBP2P_HANDLE_STATUS_REQUEST: Arc<dyn Meter> = register_meter("router_libp2p_handle_status_request");
    pub static ref LIBP2P_HANDLE_STATUS_RESPONSE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_status_response", "qps");
    pub static ref LIBP2P_HANDLE_STATUS_RESPONSE_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_status_response", "latency", 1024);

    // libp2p_event_handler: get chunks
    pub static ref LIBP2P_HANDLE_GET_CHUNKS_REQUEST: Arc<dyn Meter> = register_meter("router_libp2p_handle_get_chunks_request");
    pub static ref LIBP2P_HANDLE_GET_CHUNKS_RESPONSE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_get_chunks_response", "qps");
    pub static ref LIBP2P_HANDLE_GET_CHUNKS_RESPONSE_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_get_chunks_response", "latency", 1024);

    // libp2p_event_handler: rpc errors
    pub static ref LIBP2P_HANDLE_RESPONSE_ERROR: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_response_error", "qps");
    pub static ref LIBP2P_HANDLE_RESPONSE_ERROR_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_response_error", "latency", 1024);

    // libp2p_event_handler: find & announce file
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_file", "qps");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_pubsub_find_file", "latency", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE_TIMEOUT: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_file", "timeout");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE_STORE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_file", "store");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE_CACHE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_file", "cache");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_FILE_FORWARD: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_file", "forward");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_FILE: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_announce_file", "qps");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_FILE_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_pubsub_announce_file", "latency", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_FILE_TIMEOUT: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_announce_file", "timeout");

    // libp2p_event_handler: find & announce chunks
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_CHUNKS: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_find_chunks", "qps");
    pub static ref LIBP2P_HANDLE_PUBSUB_FIND_CHUNKS_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_pubsub_find_chunks", "latency", 1024);
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_CHUNKS: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_announce_chunks", "qps");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_CHUNKS_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_pubsub_announce_chunks", "latency", 1024);

    // libp2p_event_handler: announce shard config
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_SHARD: Arc<dyn Meter> = register_meter_with_group("router_libp2p_handle_pubsub_announce_shard", "qps");
    pub static ref LIBP2P_HANDLE_PUBSUB_ANNOUNCE_SHARD_LATENCY: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register_with_group("router_libp2p_handle_pubsub_announce_shard", "latency", 1024);

    // libp2p_event_handler: verify IP address
    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip");
    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP_UNSEEN: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip_unseen");
    pub static ref LIBP2P_VERIFY_ANNOUNCED_IP_MISMATCH: Arc<dyn Meter> = register_meter("router_libp2p_verify_announced_ip_mismatch");

    // batcher
    pub static ref BATCHER_ANNOUNCE_FILE_SIZE: Arc<dyn Histogram> = Sample::ExpDecay(0.015).register("router_batcher_announce_file_size", 1024);
}
