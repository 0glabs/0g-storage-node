use network::{NetworkMessage, PeerAction, PeerId, PubsubMessage, ReportSource};
use tokio::sync::mpsc;

pub struct SyncNetworkContext {
    network_send: mpsc::UnboundedSender<NetworkMessage>,
}

impl SyncNetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<NetworkMessage>) -> Self {
        Self { network_send }
    }

    /// Sends an arbitrary network message.
    pub fn send(&self, msg: NetworkMessage) {
        self.network_send.send(msg).unwrap_or_else(|_| {
            warn!("Could not send message to the network service");
        })
    }

    /// Publishes a single message.
    pub fn publish(&self, msg: PubsubMessage) {
        self.send(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }

    pub fn report_peer(&self, peer_id: PeerId, action: PeerAction, msg: &'static str) {
        debug!(%peer_id, ?action, %msg, "Report peer");
        self.send(NetworkMessage::ReportPeer {
            peer_id,
            action,
            source: ReportSource::SyncService,
            msg,
        })
    }

    pub fn ban_peer(&self, peer_id: PeerId, msg: &'static str) {
        info!(%peer_id, %msg, "Ban peer");
        self.send(NetworkMessage::ReportPeer {
            peer_id,
            action: PeerAction::Fatal,
            source: ReportSource::SyncService,
            msg,
        })
    }
}
