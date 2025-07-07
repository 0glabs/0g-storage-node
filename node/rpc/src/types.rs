use crate::{error, zgs_grpc_proto};
use append_merkle::ZERO_HASHES;
use ethereum_types::H256 as EthH256;
use jsonrpsee::core::RpcResult;
use merkle_light::hash::Algorithm;
use merkle_light::merkle::{log2_pow2, next_pow2, MerkleTree};
use merkle_tree::RawLeafSha3Algorithm;
use network::Multiaddr;
use serde::{Deserialize, Serialize};
use shared_types::{
    compute_padded_chunk_size, compute_segment_size, DataRoot, FileProof, NetworkIdentity,
    Transaction, CHUNK_SIZE,
};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::hash::Hasher;
use std::net::IpAddr;
use std::time::Instant;
use storage::config::ShardConfig;
use storage::log_store::log_manager::bytes_to_entries;
use storage::H256;
use tonic::Status as GrpcStatus;

const ZERO_HASH: [u8; 32] = [
    0xd3, 0x97, 0xb3, 0xb0, 0x43, 0xd8, 0x7f, 0xcd, 0x6f, 0xad, 0x12, 0x91, 0xff, 0xb, 0xfd, 0x16,
    0x40, 0x1c, 0x27, 0x48, 0x96, 0xd8, 0xc6, 0x3a, 0x92, 0x37, 0x27, 0xf0, 0x77, 0xb8, 0xe0, 0xb5,
];

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub connected_peers: usize,
    pub log_sync_height: u64,
    pub log_sync_block: H256,
    pub next_tx_seq: u64,
    pub network_identity: NetworkIdentity,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInfo {
    pub peer_id: String,
    pub listen_addresses: Vec<Multiaddr>,
    pub total_peers: usize,
    pub banned_peers: usize,
    pub disconnected_peers: usize,
    pub connected_peers: usize,
    pub connected_outgoing_peers: usize,
    pub connected_incoming_peers: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
    pub is_cached: bool,
    pub uploaded_seg_num: usize,
    /// Whether file is pruned, in which case `finalized` will be `false`.
    pub pruned: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Segment(#[serde(with = "base64")] pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentWithProof {
    /// File merkle root.
    pub root: DataRoot,
    #[serde(with = "base64")]
    /// With fixed data size except the last segment.
    pub data: Vec<u8>,
    /// Segment index.
    pub index: usize,
    /// File merkle proof whose leaf node is segment root.
    pub proof: FileProof,
    /// File size
    pub file_size: usize,
}

/// Convert the proto DataRoot → your app’s DataRoot
impl TryFrom<zgs_grpc_proto::DataRoot> for DataRoot {
    type Error = GrpcStatus;

    fn try_from(value: zgs_grpc_proto::DataRoot) -> Result<Self, GrpcStatus> {
        let bytes = value.value;
        if bytes.len() != 32 {
            return Err(GrpcStatus::invalid_argument(format!(
                "Invalid hash length: got {}, want 32",
                bytes.len()
            )));
        }
        // assume AppDataRoot is a newtype around H256:
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(EthH256(arr))
    }
}

/// Convert proto FileProof → your app’s FileProof
impl TryFrom<zgs_grpc_proto::FileProof> for FileProof {
    type Error = GrpcStatus;

    fn try_from(value: zgs_grpc_proto::FileProof) -> Result<Self, GrpcStatus> {
        // turn each `bytes` into an H256
        let mut lemma = Vec::with_capacity(value.lemma.len());
        for bin in value.lemma {
            if bin.len() != 32 {
                return Err(GrpcStatus::invalid_argument(format!(
                    "Invalid hash length: got {}, want 32",
                    bin.len()
                )));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bin);
            lemma.push(H256(arr));
        }

        Ok(FileProof {
            lemma,
            path: value.path,
        })
    }
}

/// Convert the full SegmentWithProof
impl TryFrom<zgs_grpc_proto::SegmentWithProof> for SegmentWithProof {
    type Error = GrpcStatus;

    fn try_from(grpc_segment: zgs_grpc_proto::SegmentWithProof) -> Result<Self, GrpcStatus> {
        let root = grpc_segment.root.unwrap().try_into()?;
        let data = grpc_segment.data;
        // index is u64 in proto, usize in app
        let index = grpc_segment.index.try_into().map_err(|_| {
            GrpcStatus::invalid_argument(format!("Invalid segment index: {}", grpc_segment.index))
        })?;
        let proof = grpc_segment.proof.unwrap().try_into()?;
        let file_size = grpc_segment.file_size.try_into().map_err(|_| {
            GrpcStatus::invalid_argument(format!("Invalid file size: {}", grpc_segment.file_size))
        })?;

        Ok(SegmentWithProof {
            root,
            data,
            index,
            proof,
            file_size,
        })
    }
}

impl SegmentWithProof {
    /// Splits file into segments and returns the total number of segments and the last segment size.
    pub fn split_file_into_segments(
        file_size: usize,
        chunks_per_segment: usize,
    ) -> RpcResult<(usize, usize)> {
        if file_size == 0 {
            return Err(error::invalid_params("file_size", "file is empty"));
        }

        let segment_size = chunks_per_segment * CHUNK_SIZE;
        let remaining_size = file_size % segment_size;
        let mut num_segments = file_size / segment_size;

        if remaining_size == 0 {
            return Ok((num_segments, segment_size));
        }

        // Otherwise, the last segment is not full.
        num_segments += 1;

        let last_chunk_size = remaining_size % CHUNK_SIZE;
        if last_chunk_size == 0 {
            Ok((num_segments, remaining_size))
        } else {
            // Padding last chunk with zeros.
            let last_segment_size = remaining_size - last_chunk_size + CHUNK_SIZE;
            Ok((num_segments, last_segment_size))
        }
    }

    fn validate_data_size_and_index(
        &self,
        file_size: usize,
        chunks_per_segment: usize,
    ) -> RpcResult<usize> {
        let (num_segments, last_segment_size) =
            SegmentWithProof::split_file_into_segments(file_size, chunks_per_segment)?;

        if self.index >= num_segments {
            return Err(error::invalid_params("index", "index out of bound"));
        }

        let data_size = if self.index == num_segments - 1 {
            last_segment_size
        } else {
            chunks_per_segment * CHUNK_SIZE
        };

        if self.data.len() != data_size {
            return Err(error::invalid_params("data", "invalid data length"));
        }

        Ok(num_segments)
    }

    fn calculate_segment_merkle_root(&self, extend_chunk_length: usize) -> [u8; 32] {
        let mut a = RawLeafSha3Algorithm::default();
        let hashes = self.data.chunks_exact(CHUNK_SIZE).map(|x| {
            a.reset();
            a.write(x);
            a.hash()
        });
        let mut hash_data = hashes.collect::<Vec<_>>();
        hash_data.append(&mut vec![ZERO_HASH; extend_chunk_length]);

        MerkleTree::<_, RawLeafSha3Algorithm>::new(hash_data).root()
    }

    fn validate_proof(
        &self,
        num_segments: usize,
        chunks_per_segment: usize,
        expected_data_length: usize,
    ) -> RpcResult<()> {
        // Validate proof data format at first.
        if self.proof.path.is_empty() {
            if self.proof.lemma.len() != 1 {
                return Err(error::invalid_params("proof", "invalid proof"));
            }
        } else if self.proof.lemma.len() != self.proof.path.len() + 2 {
            return Err(error::invalid_params("proof", "invalid proof"));
        }

        // Calculate segment merkle root to verify proof.
        let extend_chunk_length = if expected_data_length > self.data.len() {
            let extend_data_length = expected_data_length - self.data.len();
            if extend_data_length % CHUNK_SIZE != 0 {
                return Err(error::invalid_params("proof", "invalid data len"));
            }

            extend_data_length / CHUNK_SIZE
        } else {
            0
        };

        let segment_root = self.calculate_segment_merkle_root(extend_chunk_length);
        if !self
            .proof
            .validate(&segment_root, &self.root, self.index, num_segments)?
        {
            return Err(error::invalid_params("proof", "validation failed"));
        }

        let chunks_for_file = bytes_to_entries(self.file_size as u64) as usize;
        let (segments_for_file, _) = compute_segment_size(chunks_for_file, chunks_per_segment);
        if !self.validate_rear_padding(
            num_segments,
            segments_for_file,
            log2_pow2(chunks_per_segment),
        ) {
            return Err(error::invalid_params(
                "proof",
                "invalid proof node value in the padding range",
            ));
        }

        Ok(())
    }

    /// Validates the segment data size and proof.
    pub fn validate(&self, chunks_per_segment: usize) -> RpcResult<()> {
        self.validate_data_size_and_index(self.file_size, chunks_per_segment)?;

        let (chunks, _) = compute_padded_chunk_size(self.file_size);
        let (segments_for_proof, last_segment_size) =
            compute_segment_size(chunks, chunks_per_segment);

        let expected_data_length = if self.index == segments_for_proof - 1 {
            last_segment_size * CHUNK_SIZE
        } else {
            chunks_per_segment * CHUNK_SIZE
        };

        debug!(
            "data len: {}, expected len: {}",
            self.data.len(),
            expected_data_length
        );

        self.validate_proof(segments_for_proof, chunks_per_segment, expected_data_length)?;
        Ok(())
    }

    fn validate_rear_padding(
        &self,
        num_segments: usize,
        num_segments_for_file: usize,
        segment_tree_depth: usize,
    ) -> bool {
        let mut left_chunk_count = num_segments;
        let mut proof_position = 0;
        for (i, is_left) in self.proof.path.iter().rev().enumerate() {
            let subtree_size = next_pow2(left_chunk_count) >> 1;
            if !is_left {
                proof_position += subtree_size;
                left_chunk_count -= subtree_size;
            } else {
                // The proof node is in the right, so it's possible to be in the rear padding range.
                left_chunk_count = subtree_size;
                // If all the data within the proof node range is padded, its value should match
                // `ZERO_HASHES`.
                if proof_position + subtree_size >= num_segments_for_file {
                    let subtree_depth = log2_pow2(subtree_size) + segment_tree_depth;
                    if self.proof.lemma[self.proof.lemma.len() - 2 - i]
                        != ZERO_HASHES[subtree_depth]
                    {
                        warn!(
                            "invalid proof in the padding range: index={} get={:?} expected={:?}",
                            i,
                            self.proof.lemma[self.proof.lemma.len() - 2 - i],
                            ZERO_HASHES[subtree_depth]
                        );
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Returns the index of first chunk in the segment.
    #[allow(dead_code)]
    pub fn chunk_index(&self, chunks_per_segment: usize) -> usize {
        self.index * chunks_per_segment
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    pub client: Client,
    pub connection_status: PeerConnectionStatus,
    pub listening_addresses: Vec<Multiaddr>,
    pub seen_ips: HashSet<IpAddr>,
    pub is_trusted: bool,
    pub connection_direction: Option<String>, // Incoming/Outgoing
    pub enr: Option<String>,
}

impl From<&network::PeerInfo> for PeerInfo {
    fn from(value: &network::PeerInfo) -> Self {
        Self {
            client: value.client().clone().into(),
            connection_status: value.connection_status().clone().into(),
            listening_addresses: value.listening_addresses().clone(),
            seen_ips: value.seen_ip_addresses().collect(),
            is_trusted: value.is_trusted(),
            connection_direction: value.connection_direction().map(|x| match x {
                network::ConnectionDirection::Incoming => "Incoming".into(),
                network::ConnectionDirection::Outgoing => "Outgoing".into(),
            }),
            enr: value.enr().map(|x| x.to_base64()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationInfo {
    pub ip: IpAddr,
    pub shard_config: ShardConfig,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub version: String,
    pub os: String,
    pub protocol: String,
    pub agent: Option<String>,
}

impl From<network::Client> for Client {
    fn from(value: network::Client) -> Self {
        Self {
            version: value.version,
            os: value.os_version,
            protocol: value.protocol_version,
            agent: value.agent_string,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerConnectionStatus {
    pub status: String,
    pub connections_in: u8,
    pub connections_out: u8,
    pub last_seen_secs: u64,
}

impl PeerConnectionStatus {
    fn new(status: &str, n_in: u8, n_out: u8, last_seen: Option<Instant>) -> Self {
        Self {
            status: status.into(),
            connections_in: n_in,
            connections_out: n_out,
            last_seen_secs: last_seen.map_or(0, |x| x.elapsed().as_secs()),
        }
    }
}

impl From<network::PeerConnectionStatus> for PeerConnectionStatus {
    fn from(value: network::PeerConnectionStatus) -> Self {
        match value {
            network::PeerConnectionStatus::Connected { n_in, n_out } => {
                Self::new("connected", n_in, n_out, None)
            }
            network::PeerConnectionStatus::Disconnecting { .. } => {
                Self::new("disconnecting", 0, 0, None)
            }
            network::PeerConnectionStatus::Disconnected { since } => {
                Self::new("disconnected", 0, 0, Some(since))
            }
            network::PeerConnectionStatus::Banned { since } => {
                Self::new("banned", 0, 0, Some(since))
            }
            network::PeerConnectionStatus::Dialing { since } => {
                Self::new("dialing", 0, 0, Some(since))
            }
            network::PeerConnectionStatus::Unknown => Self::new("unknown", 0, 0, None),
        }
    }
}

mod base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Segment;

    #[test]
    fn test_segment_serde() {
        let seg = Segment("hello, world".as_bytes().to_vec());
        let result = serde_json::to_string(&seg).unwrap();
        assert_eq!(result.as_str(), "\"aGVsbG8sIHdvcmxk\"");

        let seg2: Segment = serde_json::from_str("\"aGVsbG8sIHdvcmxk\"").unwrap();
        assert_eq!(String::from_utf8(seg2.0).unwrap().as_str(), "hello, world");
    }
}
