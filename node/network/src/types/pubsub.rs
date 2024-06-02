//! Handles the encoding and decoding of pubsub messages.

use crate::types::{GossipEncoding, GossipKind, GossipTopic};
use crate::{Keypair, PublicKey, SigningError, TopicHash};
use libp2p::{
    gossipsub::{DataTransform, GossipsubMessage, RawGossipsubMessage},
    Multiaddr, PeerId,
};
use shared_types::TxID;
use snap::raw::{decompress_len, Decoder, Encoder};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::{
    io::{Error, ErrorKind},
    ops::Deref,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WrappedMultiaddr(Multiaddr);

impl From<Multiaddr> for WrappedMultiaddr {
    fn from(addr: Multiaddr) -> Self {
        WrappedMultiaddr(addr)
    }
}

impl From<WrappedMultiaddr> for Multiaddr {
    fn from(addr: WrappedMultiaddr) -> Self {
        addr.0
    }
}

impl ssz::Encode for WrappedMultiaddr {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_bytes_len(&self) -> usize {
        self.0.len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.0.to_vec().ssz_append(buf)
    }
}

impl ssz::Decode for WrappedMultiaddr {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, ssz::DecodeError> {
        // TODO: limit length
        match Multiaddr::try_from(bytes.to_vec()) {
            Ok(addr) => Ok(WrappedMultiaddr(addr)),
            Err(_) => Err(ssz::DecodeError::BytesInvalid(
                "Cannot parse multiaddr".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WrappedPeerId(PeerId);

impl Deref for WrappedPeerId {
    type Target = PeerId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PeerId> for WrappedPeerId {
    fn from(addr: PeerId) -> Self {
        WrappedPeerId(addr)
    }
}

impl From<WrappedPeerId> for PeerId {
    fn from(addr: WrappedPeerId) -> Self {
        addr.0
    }
}

impl ssz::Encode for WrappedPeerId {
    fn is_ssz_fixed_len() -> bool {
        // TODO: we can probably encode PeerId as fixed-length
        false
    }

    fn ssz_bytes_len(&self) -> usize {
        self.0.to_bytes().len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.0.to_bytes().ssz_append(buf)
    }
}

impl ssz::Decode for WrappedPeerId {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, ssz::DecodeError> {
        // TODO: limit length
        match PeerId::from_bytes(bytes) {
            Ok(addr) => Ok(WrappedPeerId(addr)),
            Err(_) => Err(ssz::DecodeError::BytesInvalid(
                "Cannot parse peer id".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct FindFile {
    pub tx_id: TxID,
    pub timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct FindChunks {
    pub tx_id: TxID,
    pub index_start: u64, // inclusive
    pub index_end: u64,   // exclusive
    pub timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct AnnounceFile {
    pub tx_id: TxID,
    pub num_shard: usize,
    pub shard_id: usize,
    pub peer_id: WrappedPeerId,
    pub at: WrappedMultiaddr,
    pub timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct AnnounceChunks {
    pub tx_id: TxID,
    pub index_start: u64, // inclusive
    pub index_end: u64,   // exclusive
    pub peer_id: WrappedPeerId,
    pub at: WrappedMultiaddr,
    pub timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct AnnounceShardConfig {
    pub num_shard: usize,
    pub shard_id: usize,
    pub peer_id: WrappedPeerId,
    pub at: WrappedMultiaddr,
    pub timestamp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SignedMessage<T: Encode + Decode> {
    pub inner: T,
    pub signature: Vec<u8>,
    pub resend_timestamp: u32,
}

impl<T: Encode + Decode> SignedMessage<T> {
    pub fn sign_message(msg: T, keypair: &Keypair) -> Result<SignedMessage<T>, SigningError> {
        let raw = msg.as_ssz_bytes();
        let signature = keypair.sign(&raw)?;

        Ok(SignedMessage {
            inner: msg,
            signature,
            resend_timestamp: 0,
        })
    }
}

impl<T: Encode + Decode> Deref for SignedMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub trait HasSignature {
    fn verify_signature(&self, public_key: &PublicKey) -> bool;
}

impl<T: Encode + Decode> HasSignature for SignedMessage<T> {
    fn verify_signature(&self, public_key: &PublicKey) -> bool {
        let raw = self.inner.as_ssz_bytes();
        public_key.verify(&raw, &self.signature)
    }
}

pub type SignedAnnounceFile = SignedMessage<AnnounceFile>;
pub type SignedAnnounceShardConfig = SignedMessage<AnnounceShardConfig>;
pub type SignedAnnounceChunks = SignedMessage<AnnounceChunks>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubMessage {
    ExampleMessage(u64),
    FindFile(FindFile),
    FindChunks(FindChunks),
    AnnounceFile(SignedAnnounceFile),
    AnnounceShardConfig(SignedAnnounceShardConfig),
    AnnounceChunks(SignedAnnounceChunks),
}

// Implements the `DataTransform` trait of gossipsub to employ snappy compression
pub struct SnappyTransform {
    /// Sets the maximum size we allow gossipsub messages to decompress to.
    max_size_per_message: usize,
}

impl SnappyTransform {
    pub fn new(max_size_per_message: usize) -> Self {
        SnappyTransform {
            max_size_per_message,
        }
    }
}

impl DataTransform for SnappyTransform {
    // Provides the snappy decompression from RawGossipsubMessages
    fn inbound_transform(
        &self,
        raw_message: RawGossipsubMessage,
    ) -> Result<GossipsubMessage, std::io::Error> {
        // check the length of the raw bytes
        let len = decompress_len(&raw_message.data)?;
        if len > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy decoded data > GOSSIP_MAX_SIZE",
            ));
        }

        let mut decoder = Decoder::new();
        let decompressed_data = decoder.decompress_vec(&raw_message.data)?;

        // Build the GossipsubMessage struct
        Ok(GossipsubMessage {
            source: raw_message.source,
            data: decompressed_data,
            sequence_number: raw_message.sequence_number,
            topic: raw_message.topic,
        })
    }

    /// Provides the snappy compression logic to gossipsub.
    fn outbound_transform(
        &self,
        _topic: &TopicHash,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, std::io::Error> {
        // Currently we are not employing topic-based compression. Everything is expected to be
        // snappy compressed.
        if data.len() > self.max_size_per_message {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "ssz_snappy Encoded data > GOSSIP_MAX_SIZE",
            ));
        }
        let mut encoder = Encoder::new();
        encoder.compress_vec(&data).map_err(Into::into)
    }
}

impl PubsubMessage {
    /// Returns the topics that each pubsub message will be sent across, given a supported
    /// gossipsub encoding and fork version.
    pub fn topics(&self, encoding: GossipEncoding) -> Vec<GossipTopic> {
        vec![GossipTopic::new(self.kind(), encoding)]
    }

    /// Returns the kind of gossipsub topic associated with the message.
    pub fn kind(&self) -> GossipKind {
        match self {
            PubsubMessage::ExampleMessage(_) => GossipKind::Example,
            PubsubMessage::FindFile(_) => GossipKind::FindFile,
            PubsubMessage::FindChunks(_) => GossipKind::FindChunks,
            PubsubMessage::AnnounceFile(_) => GossipKind::AnnounceFile,
            PubsubMessage::AnnounceChunks(_) => GossipKind::AnnounceChunks,
            PubsubMessage::AnnounceShardConfig(_) => GossipKind::AnnounceShardConfig,
        }
    }

    /// This decodes `data` into a `PubsubMessage` given a topic.
    /* Note: This is assuming we are not hashing topics. If we choose to hash topics, these will
     * need to be modified.
     */
    pub fn decode(topic: &TopicHash, data: &[u8]) -> Result<Self, String> {
        match GossipTopic::decode(topic.as_str()) {
            Err(_) => Err(format!("Unknown gossipsub topic: {:?}", topic)),
            Ok(gossip_topic) => {
                // All topics are currently expected to be compressed and decompressed with snappy.
                // This is done in the `SnappyTransform` struct.
                // Therefore compression has already been handled for us by the time we are
                // decoding the objects here.

                // the ssz decoders
                match gossip_topic.kind() {
                    GossipKind::Example => Ok(PubsubMessage::ExampleMessage(
                        u64::from_ssz_bytes(data).map_err(|e| format!("{:?}", e))?,
                    )),
                    GossipKind::FindFile => Ok(PubsubMessage::FindFile(
                        FindFile::from_ssz_bytes(data).map_err(|e| format!("{:?}", e))?,
                    )),
                    GossipKind::FindChunks => Ok(PubsubMessage::FindChunks(
                        FindChunks::from_ssz_bytes(data).map_err(|e| format!("{:?}", e))?,
                    )),
                    GossipKind::AnnounceFile => Ok(PubsubMessage::AnnounceFile(
                        SignedAnnounceFile::from_ssz_bytes(data).map_err(|e| format!("{:?}", e))?,
                    )),
                    GossipKind::AnnounceChunks => Ok(PubsubMessage::AnnounceChunks(
                        SignedAnnounceChunks::from_ssz_bytes(data)
                            .map_err(|e| format!("{:?}", e))?,
                    )),
                    GossipKind::AnnounceShardConfig => Ok(PubsubMessage::AnnounceShardConfig(
                        SignedAnnounceShardConfig::from_ssz_bytes(data)
                            .map_err(|e| format!("{:?}", e))?,
                    )),
                }
            }
        }
    }

    /// Encodes a `PubsubMessage` based on the topic encodings. The first known encoding is used. If
    /// no encoding is known, and error is returned.
    pub fn encode(&self, _encoding: GossipEncoding) -> Vec<u8> {
        // Currently do not employ encoding strategies based on the topic. All messages are ssz
        // encoded.
        // Also note, that the compression is handled by the `SnappyTransform` struct. Gossipsub will compress the
        // messages for us.
        match &self {
            PubsubMessage::ExampleMessage(data) => data.as_ssz_bytes(),
            PubsubMessage::FindFile(data) => data.as_ssz_bytes(),
            PubsubMessage::FindChunks(data) => data.as_ssz_bytes(),
            PubsubMessage::AnnounceFile(data) => data.as_ssz_bytes(),
            PubsubMessage::AnnounceChunks(data) => data.as_ssz_bytes(),
            PubsubMessage::AnnounceShardConfig(data) => data.as_ssz_bytes(),
        }
    }
}

impl std::fmt::Display for PubsubMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubsubMessage::ExampleMessage(msg) => {
                write!(f, "Example message: {}", msg)
            }
            PubsubMessage::FindFile(msg) => {
                write!(f, "FindFile message: {:?}", msg)
            }
            PubsubMessage::FindChunks(msg) => {
                write!(f, "FindChunks message: {:?}", msg)
            }
            PubsubMessage::AnnounceFile(msg) => {
                write!(f, "AnnounceFile message: {:?}", msg)
            }
            PubsubMessage::AnnounceChunks(msg) => {
                write!(f, "AnnounceChunks message: {:?}", msg)
            }
            PubsubMessage::AnnounceShardConfig(msg) => {
                write!(f, "AnnounceShardConfig message: {:?}", msg)
            }
        }
    }
}
