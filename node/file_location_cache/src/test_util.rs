use network::{
    libp2p::identity,
    types::{AnnounceFile, SignedAnnounceFile, SignedMessage, TimedMessage},
    Multiaddr, PeerId,
};
use shared_types::{timestamp_now, TxID};

#[derive(Default)]
pub struct AnnounceFileBuilder {
    tx_id: Option<TxID>,
    peer_id: Option<PeerId>,
    timestamp: Option<u32>,
}

impl AnnounceFileBuilder {
    pub fn with_tx_id(mut self, tx_id: TxID) -> Self {
        self.tx_id = Some(tx_id);
        self
    }

    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn with_timestamp(mut self, timestamp: u32) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn build(self) -> SignedAnnounceFile {
        let tx_id = self.tx_id.unwrap_or_else(|| TxID::random_hash(0));
        let peer_id = self.peer_id.unwrap_or_else(PeerId::random);
        let at: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        let timestamp = self.timestamp.unwrap_or_else(timestamp_now);

        let msg = TimedMessage {
            inner: AnnounceFile {
                tx_ids: vec![tx_id],
                shard_config: Default::default(),
                peer_id: peer_id.into(),
                at: at.into(),
            },
            timestamp,
        };

        let keypair = identity::Keypair::generate_secp256k1();
        SignedMessage::sign_message(msg, &keypair).unwrap()
    }
}
