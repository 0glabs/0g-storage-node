use libp2p::gossipsub::IdentTopic as Topic;
use serde_derive::{Deserialize, Serialize};
use strum::AsRefStr;

/// The gossipsub topic names.
// These constants form a topic name of the form /TOPIC_PREFIX/TOPIC/ENCODING_POSTFIX
// For example /eth2/beacon_block/ssz
pub const TOPIC_PREFIX: &str = "eth2";
pub const SSZ_SNAPPY_ENCODING_POSTFIX: &str = "ssz_snappy";
pub const EXAMPLE_TOPIC: &str = "example";
pub const FIND_FILE_TOPIC: &str = "find_file";
pub const FIND_CHUNKS_TOPIC: &str = "find_chunks";
pub const ANNOUNCE_FILE_TOPIC: &str = "announce_file";
pub const ANNOUNCE_CHUNKS_TOPIC: &str = "announce_chunks";
pub const ANNOUNCE_SHARD_CONFIG_TOPIC: &str = "announce_shard_config";

pub const CORE_TOPICS: [GossipKind; 4] = [
    GossipKind::FindFile,
    GossipKind::FindChunks,
    GossipKind::AnnounceFile,
    GossipKind::AnnounceChunks,
];

/// A gossipsub topic which encapsulates the type of messages that should be sent and received over
/// the pubsub protocol and the way the messages should be encoded.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GossipTopic {
    /// The encoding of the topic.
    encoding: GossipEncoding,
    /// The kind of topic.
    kind: GossipKind,
}

/// Enum that brings these topics into the rust type system.
// NOTE: There is intentionally no unknown type here. We only allow known gossipsub topics.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum GossipKind {
    Example,
    FindFile,
    FindChunks,
    AnnounceFile,
    AnnounceShardConfig,
    AnnounceChunks,
}

/// The known encoding types for gossipsub messages.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum GossipEncoding {
    /// Messages are encoded with SSZSnappy.
    #[default]
    SSZSnappy,
}

impl GossipTopic {
    pub fn new(kind: GossipKind, encoding: GossipEncoding) -> Self {
        GossipTopic { encoding, kind }
    }

    /// Returns the encoding type for the gossipsub topic.
    pub fn encoding(&self) -> &GossipEncoding {
        &self.encoding
    }

    /// Returns the kind of message expected on the gossipsub topic.
    pub fn kind(&self) -> &GossipKind {
        &self.kind
    }

    pub fn decode(topic: &str) -> Result<Self, String> {
        let topic_parts: Vec<&str> = topic.split('/').collect();
        if topic_parts.len() == 4 && topic_parts[1] == TOPIC_PREFIX {
            let encoding = match topic_parts[3] {
                SSZ_SNAPPY_ENCODING_POSTFIX => GossipEncoding::SSZSnappy,
                _ => return Err(format!("Unknown encoding: {}", topic)),
            };

            let kind = match topic_parts[2] {
                EXAMPLE_TOPIC => GossipKind::Example,
                FIND_FILE_TOPIC => GossipKind::FindFile,
                FIND_CHUNKS_TOPIC => GossipKind::FindChunks,
                ANNOUNCE_FILE_TOPIC => GossipKind::AnnounceFile,
                ANNOUNCE_CHUNKS_TOPIC => GossipKind::AnnounceChunks,
                ANNOUNCE_SHARD_CONFIG_TOPIC => GossipKind::AnnounceShardConfig,
                _ => return Err(format!("Unknown topic: {}", topic)),
            };

            return Ok(GossipTopic { encoding, kind });
        }

        Err(format!("Unknown topic: {}", topic))
    }
}

impl From<GossipTopic> for Topic {
    fn from(topic: GossipTopic) -> Topic {
        Topic::new(topic)
    }
}

impl From<GossipTopic> for String {
    fn from(topic: GossipTopic) -> String {
        let encoding = match topic.encoding {
            GossipEncoding::SSZSnappy => SSZ_SNAPPY_ENCODING_POSTFIX,
        };

        let kind = match topic.kind {
            GossipKind::Example => EXAMPLE_TOPIC,
            GossipKind::FindFile => FIND_FILE_TOPIC,
            GossipKind::FindChunks => FIND_CHUNKS_TOPIC,
            GossipKind::AnnounceFile => ANNOUNCE_FILE_TOPIC,
            GossipKind::AnnounceChunks => ANNOUNCE_CHUNKS_TOPIC,
            GossipKind::AnnounceShardConfig => ANNOUNCE_SHARD_CONFIG_TOPIC,
        };

        format!("/{}/{}/{}", TOPIC_PREFIX, kind, encoding)
    }
}

impl std::fmt::Display for GossipTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let encoding = match self.encoding {
            GossipEncoding::SSZSnappy => SSZ_SNAPPY_ENCODING_POSTFIX,
        };

        let kind = match self.kind {
            GossipKind::Example => EXAMPLE_TOPIC,
            GossipKind::FindFile => FIND_FILE_TOPIC,
            GossipKind::FindChunks => FIND_CHUNKS_TOPIC,
            GossipKind::AnnounceFile => ANNOUNCE_FILE_TOPIC,
            GossipKind::AnnounceChunks => ANNOUNCE_CHUNKS_TOPIC,
            GossipKind::AnnounceShardConfig => ANNOUNCE_SHARD_CONFIG_TOPIC,
        };

        write!(f, "/{}/{}/{}", TOPIC_PREFIX, kind, encoding)
    }
}

#[cfg(test)]
mod tests {
    use super::GossipKind::*;
    use super::*;

    const BAD_PREFIX: &str = "tezos";
    const BAD_ENCODING: &str = "rlp";
    const BAD_KIND: &str = "blocks";

    fn topics() -> Vec<String> {
        let mut topics = Vec::new();

        for encoding in [GossipEncoding::SSZSnappy].iter() {
            {
                let kind = &Example;
                topics.push(GossipTopic::new(kind.clone(), encoding.clone()).into());
            }
        }
        topics
    }

    fn create_topic(prefix: &str, kind: &str, encoding: &str) -> String {
        format!("/{}/{}/{}", prefix, kind, encoding)
    }

    #[test]
    fn test_decode() {
        for topic in topics().iter() {
            assert!(GossipTopic::decode(topic.as_str()).is_ok());
        }
    }

    #[test]
    fn test_decode_malicious() {
        let bad_prefix_str = create_topic(BAD_PREFIX, EXAMPLE_TOPIC, SSZ_SNAPPY_ENCODING_POSTFIX);
        assert!(GossipTopic::decode(bad_prefix_str.as_str()).is_err());

        let bad_kind_str = create_topic(TOPIC_PREFIX, BAD_KIND, SSZ_SNAPPY_ENCODING_POSTFIX);
        assert!(GossipTopic::decode(bad_kind_str.as_str()).is_err());

        let bad_encoding_str = create_topic(TOPIC_PREFIX, EXAMPLE_TOPIC, BAD_ENCODING);
        assert!(GossipTopic::decode(bad_encoding_str.as_str()).is_err());

        // Extra parts
        assert!(
            GossipTopic::decode("/eth2/beacon_block/ssz_snappy/yolo").is_err(),
            "should have exactly 4 parts"
        );
        // Empty string
        assert!(GossipTopic::decode("").is_err());
        // Empty parts
        assert!(GossipTopic::decode("////").is_err());
    }

    #[test]
    fn test_as_str_ref() {
        assert_eq!("example", Example.as_ref());
    }
}
