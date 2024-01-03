use crate::error;
use jsonrpsee::core::RpcResult;
use merkle_light::hash::Algorithm;
use merkle_light::merkle::MerkleTree;
use merkle_tree::RawLeafSha3Algorithm;
use serde::{Deserialize, Serialize};
use shared_types::{
    compute_padded_chunk_size, compute_segment_size, DataRoot, FileProof, Transaction, CHUNK_SIZE,
};
use std::hash::Hasher;
use storage::H256;

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
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInfo {
    pub peer_id: String,
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

    fn validate_proof(&self, num_segments: usize, expected_data_length: usize) -> RpcResult<()> {
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

        self.validate_proof(segments_for_proof, expected_data_length)?;
        Ok(())
    }

    /// Returns the index of first chunk in the segment.
    #[allow(dead_code)]
    pub fn chunk_index(&self, chunks_per_segment: usize) -> usize {
        self.index * chunks_per_segment
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
