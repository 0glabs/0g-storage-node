use crate::{compute_segment_merkle_root, ChunkArrayWithProof, FileProof, Transaction, CHUNK_SIZE};
use anyhow::{bail, Result};
use append_merkle::{Algorithm, Sha3Algorithm};
use ethereum_types::H256;
use merkle_light::merkle::log2_pow2;

enum NodeProofLeaf {
    /// Segment in a single sub-tree.
    Full { node_depth: usize },

    /// Segment is not full and is made up of one or multiple sub-trees
    /// that smaller than a full segment.
    Partial,
}

/// NodeProof represents a merkle proof from submission node to file root.
struct NodeProof {
    proof: FileProof,
    leaf: NodeProofLeaf,
}

impl NodeProof {
    fn compute_segment_proof(
        self,
        segment: &ChunkArrayWithProof,
        chunks_per_segment: usize,
    ) -> Result<FileProof> {
        let mut node_depth = match self.leaf {
            // In this case, some proof pathes missed between segment
            // root and submission node, which could be retrieved from
            // the flow proof.
            NodeProofLeaf::Full { node_depth } => node_depth,

            // Client could compute the segment root via returned
            // segment data, and use the NodeProof to validate.
            NodeProofLeaf::Partial => return Ok(self.proof),
        };

        // node depth in Transaction is by number of nodes,
        // here we use depth by number of edges.
        node_depth -= 1;

        self.compute_segment_proof_full(node_depth, segment, chunks_per_segment)
    }

    fn compute_segment_proof_full(
        self,
        node_depth: usize, // node depth by edge
        segment: &ChunkArrayWithProof,
        chunks_per_segment: usize,
    ) -> Result<FileProof> {
        // when segment equals to sub-tree, just return the proof directly
        let segment_depth = log2_pow2(chunks_per_segment);
        if node_depth == segment_depth {
            return Ok(self.proof);
        }

        // otherwise, segment should be smaller than sub-tree
        assert!(node_depth > segment_depth);

        // flow tree should not be smaller than any sub-tree
        let flow_proof_path_len = segment.proof.left_proof.path().len();
        if flow_proof_path_len < node_depth {
            bail!(
                "flow proof path too small, path_len = {}, node_depth = {}",
                flow_proof_path_len,
                node_depth
            );
        }

        // segment root as proof leaf
        let segment_root: H256 =
            compute_segment_merkle_root(&segment.chunks.data, chunks_per_segment).into();
        let mut lemma = vec![segment_root];
        let mut path = vec![];

        // copy from flow proof
        for i in segment_depth..node_depth {
            lemma.push(segment.proof.left_proof.lemma()[i + 1]);
            path.push(segment.proof.left_proof.path()[i]);
        }

        // combine with node proof
        if self.proof.path.is_empty() {
            // append file root only
            lemma.push(self.proof.lemma[0]);
        } else {
            // append lemma/path and file root, and ignore the sub-tree root
            // which could be constructed via proof.
            lemma.extend_from_slice(&self.proof.lemma[1..]);
            path.extend_from_slice(&self.proof.path);
        }

        Ok(FileProof::new(lemma, path))
    }
}

impl Transaction {
    /// Computes file merkle proof for the specified segment.
    ///
    /// The leaf of proof is segment root, and root of proof is file merkle root.
    pub fn compute_segment_proof(
        &self,
        segment: &ChunkArrayWithProof,
        chunks_per_segment: usize,
    ) -> Result<FileProof> {
        // validate index
        let chunk_start_index = segment.chunks.start_index as usize;
        if chunk_start_index % chunks_per_segment != 0 {
            bail!("start index not aligned");
        }

        let total_entries = self.num_entries();
        if chunk_start_index >= total_entries {
            bail!("start index out of bound");
        }

        let data_len = segment.chunks.data.len();
        if chunk_start_index + data_len / CHUNK_SIZE > total_entries {
            bail!("end index out of bound");
        }

        // compute merkle proof from node root to file root
        let node_proof = self.compute_node_proof(chunk_start_index, chunks_per_segment);

        node_proof.compute_segment_proof(segment, chunks_per_segment)
    }

    fn compute_node_proof(&self, chunk_start_index: usize, chunks_per_segment: usize) -> NodeProof {
        // construct `lemma` in the order: root -> interier nodes -> leaf,
        // and reverse the `lemma` and `path` to create proof.
        let mut lemma = vec![self.data_merkle_root];
        let mut path = vec![];

        // Basically, a segment should be in a sub-tree except the last segment.
        // As for the last segment, it should be also in a sub-tree if the smallest
        // sub-tree is bigger than a segment. Otherwise, segment is made up of
        // all sub-trees that smaller than a segment.

        // try to find a single node for segment
        let mut node = None;
        let mut total_chunks = 0;
        for (depth, root) in self.merkle_nodes.iter().cloned() {
            let node_chunks = Transaction::num_entries_of_node(depth);

            // ignore nodes that smaller than a segment
            if node_chunks < chunks_per_segment {
                break;
            }

            total_chunks += node_chunks;

            // segment in a single node
            if chunk_start_index < total_chunks {
                node = Some((depth, root));
                break;
            }

            // otherwise, segment not in current node
            lemma.push(root);
            path.push(false);
        }

        let leaf = match node {
            // segment in a single node
            Some((depth, root)) => {
                // append lemma and path if right sibling exists
                if let Some(right_root) = self.compute_merkle_root(depth) {
                    lemma.push(right_root);
                    path.push(true);
                }

                // append leaf
                lemma.push(root);

                NodeProofLeaf::Full { node_depth: depth }
            }

            // segment is made up of multiple nodes
            None => {
                let segment_depth = log2_pow2(chunks_per_segment) + 1;
                let right_root = self
                    .compute_merkle_root(segment_depth)
                    .expect("merkle root should be exists");

                // append leaf
                lemma.push(right_root);

                NodeProofLeaf::Partial
            }
        };

        // change to bottom-top order: leaf -> (interiers) -> root
        lemma.reverse();
        path.reverse();

        NodeProof {
            proof: FileProof::new(lemma, path),
            leaf,
        }
    }

    /// Computes the merkle root of nodes that of depth less than `max_depth_exclusive`.
    fn compute_merkle_root(&self, max_depth_exclusive: usize) -> Option<H256> {
        let (depth, mut root) = self.merkle_nodes.last().cloned()?;
        if depth >= max_depth_exclusive {
            return None;
        }

        for (node_depth, node_root) in self.merkle_nodes.iter().rev().skip(1) {
            if *node_depth >= max_depth_exclusive {
                break;
            }

            root = Sha3Algorithm::parent(node_root, &root);
        }

        Some(root)
    }
}
