mod merkle_tree;
mod proof;
mod sha3;

use anyhow::{anyhow, bail, Result};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::{trace, warn};

pub use crate::merkle_tree::{
    Algorithm, HashElement, MerkleTreeInitialData, MerkleTreeRead, ZERO_HASHES,
};
pub use proof::{Proof, RangeProof};
pub use sha3::Sha3Algorithm;

pub struct AppendMerkleTree<E: HashElement, A: Algorithm<E>> {
    /// Keep all the nodes in the latest version. `layers[0]` is the layer of leaves.
    layers: Vec<Vec<E>>,
    /// Keep the delta nodes that can be used to construct a history tree.
    /// The key is the root node of that version.
    delta_nodes_map: BTreeMap<u64, DeltaNodes<E>>,
    root_to_tx_seq_map: HashMap<E, u64>,

    /// For `last_chunk_merkle` after the first chunk, this is set to `Some(10)` so that
    /// `revert_to` can reset the state correctly when needed.
    min_depth: Option<usize>,
    /// Used to compute the correct padding hash.
    /// 0 for `pora_chunk_merkle` and 10 for not-first `last_chunk_merkle`.
    leaf_height: usize,
    _a: PhantomData<A>,
}

impl<E: HashElement, A: Algorithm<E>> AppendMerkleTree<E, A> {
    pub fn new(leaves: Vec<E>, leaf_height: usize, start_tx_seq: Option<u64>) -> Self {
        let mut merkle = Self {
            layers: vec![leaves],
            delta_nodes_map: BTreeMap::new(),
            root_to_tx_seq_map: HashMap::new(),
            min_depth: None,
            leaf_height,
            _a: Default::default(),
        };
        if merkle.leaves() == 0 {
            if let Some(seq) = start_tx_seq {
                merkle.delta_nodes_map.insert(
                    seq,
                    DeltaNodes {
                        right_most_nodes: vec![],
                    },
                );
            }
            return merkle;
        }
        // Reconstruct the whole tree.
        merkle.recompute(0, 0, None);
        // Commit the first version in memory.
        // TODO(zz): Check when the roots become available.
        merkle.commit(start_tx_seq);
        merkle
    }

    pub fn new_with_subtrees(
        initial_data: MerkleTreeInitialData<E>,
        leaf_height: usize,
        start_tx_seq: Option<u64>,
    ) -> Result<Self> {
        let mut merkle = Self {
            layers: vec![vec![]],
            delta_nodes_map: BTreeMap::new(),
            root_to_tx_seq_map: HashMap::new(),
            min_depth: None,
            leaf_height,
            _a: Default::default(),
        };
        if initial_data.subtree_list.is_empty() {
            if let Some(seq) = start_tx_seq {
                merkle.delta_nodes_map.insert(
                    seq,
                    DeltaNodes {
                        right_most_nodes: vec![],
                    },
                );
            }
            return Ok(merkle);
        }
        merkle.append_subtree_list(initial_data.subtree_list)?;
        merkle.commit(start_tx_seq);
        for (index, h) in initial_data.known_leaves {
            merkle.fill_leaf(index, h);
        }
        for (layer_index, position, h) in initial_data.extra_mpt_nodes {
            // TODO: Delete duplicate nodes from DB.
            merkle.layers[layer_index][position] = h;
        }
        Ok(merkle)
    }

    /// This is only used for the last chunk, so `leaf_height` is always 0 so far.
    pub fn new_with_depth(leaves: Vec<E>, depth: usize, start_tx_seq: Option<u64>) -> Self {
        if leaves.is_empty() {
            // Create an empty merkle tree with `depth`.
            let mut merkle = Self {
                layers: vec![vec![]; depth],
                delta_nodes_map: BTreeMap::new(),
                root_to_tx_seq_map: HashMap::new(),
                min_depth: Some(depth),
                leaf_height: 0,
                _a: Default::default(),
            };
            if let Some(seq) = start_tx_seq {
                merkle.delta_nodes_map.insert(
                    seq,
                    DeltaNodes {
                        right_most_nodes: vec![],
                    },
                );
            }
            merkle
        } else {
            let mut layers = vec![vec![]; depth];
            layers[0] = leaves;
            let mut merkle = Self {
                layers,
                delta_nodes_map: BTreeMap::new(),
                root_to_tx_seq_map: HashMap::new(),
                min_depth: Some(depth),
                leaf_height: 0,
                _a: Default::default(),
            };
            // Reconstruct the whole tree.
            merkle.recompute(0, 0, None);
            // Commit the first version in memory.
            merkle.commit(start_tx_seq);
            merkle
        }
    }

    /// Return the new merkle root.
    pub fn append(&mut self, new_leaf: E) {
        self.layers[0].push(new_leaf);
        self.recompute_after_append_leaves(self.leaves() - 1);
    }

    pub fn append_list(&mut self, mut leaf_list: Vec<E>) {
        let start_index = self.leaves();
        self.layers[0].append(&mut leaf_list);
        self.recompute_after_append_leaves(start_index);
    }

    /// Append a leaf list by providing their intermediate node hash.
    /// The appended subtree must be aligned. And it's up to the caller to
    /// append the padding nodes for alignment.
    /// Other nodes in the subtree will be set to `null` nodes.
    /// TODO: Optimize to avoid storing the `null` nodes?
    pub fn append_subtree(&mut self, subtree_depth: usize, subtree_root: E) -> Result<()> {
        let start_index = self.leaves();
        self.append_subtree_inner(subtree_depth, subtree_root)?;
        self.recompute_after_append_subtree(start_index, subtree_depth - 1);
        Ok(())
    }

    pub fn append_subtree_list(&mut self, subtree_list: Vec<(usize, E)>) -> Result<()> {
        for (subtree_depth, subtree_root) in subtree_list {
            let start_index = self.leaves();
            self.append_subtree_inner(subtree_depth, subtree_root)?;
            self.recompute_after_append_subtree(start_index, subtree_depth - 1);
        }
        Ok(())
    }

    /// Change the value of the last leaf and return the new merkle root.
    /// This is needed if our merkle-tree in memory only keeps intermediate nodes instead of real leaves.
    pub fn update_last(&mut self, updated_leaf: E) {
        if self.layers[0].is_empty() {
            // Special case for the first data.
            self.layers[0].push(updated_leaf);
        } else {
            *self.layers[0].last_mut().unwrap() = updated_leaf;
        }
        self.recompute_after_append_leaves(self.leaves() - 1);
    }

    /// Fill an unknown `null` leaf with its real value.
    /// Panics if the leaf changes the merkle root or the index is out of range.
    /// TODO: Batch computing intermediate nodes.
    pub fn fill_leaf(&mut self, index: usize, leaf: E) {
        if self.layers[0][index] == E::null() {
            self.layers[0][index] = leaf;
            self.recompute_after_fill_leaves(index, index + 1);
        } else if self.layers[0][index] != leaf {
            panic!(
                "Fill with invalid leaf, index={} was={:?} get={:?}",
                index, self.layers[0][index], leaf
            );
        }
    }

    /// Fill nodes with a valid proof data.
    /// This requires that the proof is built against this tree.
    /// This should only be called after validating the proof (including checking root existence).
    /// Returns `Error` if the data is conflict with existing ones.
    pub fn fill_with_range_proof(
        &mut self,
        proof: RangeProof<E>,
    ) -> Result<Vec<(usize, usize, E)>> {
        self.fill_with_proof(
            proof
                .left_proof
                .proof_nodes_in_tree()
                .split_off(self.leaf_height),
        )?;
        self.fill_with_proof(
            proof
                .right_proof
                .proof_nodes_in_tree()
                .split_off(self.leaf_height),
        )
    }

    pub fn fill_with_file_proof(
        &mut self,
        proof: Proof<E>,
        mut tx_merkle_nodes: Vec<(usize, E)>,
        start_index: u64,
    ) -> Result<Vec<(usize, usize, E)>> {
        let tx_merkle_nodes_size = tx_merkle_nodes.len();
        if self.leaf_height != 0 {
            tx_merkle_nodes = tx_merkle_nodes
                .into_iter()
                .filter_map(|(height, data)| {
                    if height > self.leaf_height {
                        Some((height - self.leaf_height - 1, data))
                    } else {
                        None
                    }
                })
                .collect();
        }
        if tx_merkle_nodes.is_empty() {
            return Ok(Vec::new());
        }
        let mut position_and_data =
            proof.file_proof_nodes_in_tree(tx_merkle_nodes, tx_merkle_nodes_size);
        let start_index = (start_index >> self.leaf_height) as usize;
        for (i, (position, _)) in position_and_data.iter_mut().enumerate() {
            *position += start_index >> i;
        }
        self.fill_with_proof(position_and_data)
    }

    /// This assumes that the proof leaf is no lower than the tree leaf. It holds for both SegmentProof and ChunkProof.
    /// Return the inserted nodes and position.
    fn fill_with_proof(
        &mut self,
        position_and_data: Vec<(usize, E)>,
    ) -> Result<Vec<(usize, usize, E)>> {
        let mut updated_nodes = Vec::new();
        // A valid proof should not fail the following checks.
        for (i, (position, data)) in position_and_data.into_iter().enumerate() {
            let layer = &mut self.layers[i];
            if position > layer.len() {
                bail!(
                    "proof position out of range, position={} layer.len()={}",
                    position,
                    layer.len()
                );
            }
            if position == layer.len() {
                // skip padding node.
                continue;
            }
            if layer[position] == E::null() {
                layer[position] = data.clone();
                updated_nodes.push((i, position, data))
            } else if layer[position] != data {
                // The last node in each layer may have changed in the tree.
                trace!(
                    "conflict data layer={} position={} tree_data={:?} proof_data={:?}",
                    i,
                    position,
                    layer[position],
                    data
                );
            }
        }
        Ok(updated_nodes)
    }

    pub fn check_root(&self, root: &E) -> bool {
        self.root_to_tx_seq_map.contains_key(root)
    }

    pub fn leaf_at(&self, position: usize) -> Result<Option<E>> {
        if position >= self.leaves() {
            bail!("Out of bound: position={} end={}", position, self.leaves());
        }
        if self.layers[0][position] != E::null() {
            Ok(Some(self.layers[0][position].clone()))
        } else {
            // The leaf hash is unknown.
            Ok(None)
        }
    }

    /// Return a list of subtrees that can be used to rebuild the tree.
    pub fn get_subtrees(&self) -> Vec<(usize, E)> {
        let mut next_index = 0;
        let mut subtree_list: Vec<(usize, E)> = Vec::new();
        while next_index < self.leaves() {
            let root_tuple = self.first_known_root_at(next_index);
            let subtree_size = 1 << (root_tuple.0 - 1);
            let root_start_index = next_index / subtree_size * subtree_size;

            // Previous subtrees are included within the new subtree.
            // Pop them out and replace with the new one.
            if root_start_index < next_index {
                while let Some(last) = subtree_list.pop() {
                    next_index -= 1 << (last.0 - 1);
                    if next_index == root_start_index {
                        break;
                    }
                }
            }
            next_index += subtree_size;
            subtree_list.push(root_tuple);
        }
        subtree_list
    }
}

impl<E: HashElement, A: Algorithm<E>> AppendMerkleTree<E, A> {
    pub fn commit(&mut self, tx_seq: Option<u64>) {
        if let Some(tx_seq) = tx_seq {
            if self.leaves() == 0 {
                // The state is empty, so we just save the root as `null`.
                // Note that this root should not be used.
                self.delta_nodes_map.insert(
                    tx_seq,
                    DeltaNodes {
                        right_most_nodes: vec![],
                    },
                );
                return;
            }
            let mut right_most_nodes = Vec::new();
            for layer in &self.layers {
                right_most_nodes.push((layer.len() - 1, layer.last().unwrap().clone()));
            }
            let root = self.root().clone();
            assert_eq!(root, right_most_nodes.last().unwrap().1);
            self.delta_nodes_map
                .insert(tx_seq, DeltaNodes::new(right_most_nodes));
            self.root_to_tx_seq_map.insert(root, tx_seq);
        }
    }

    fn before_extend_layer(&mut self, height: usize) {
        if height == self.layers.len() {
            self.layers.push(Vec::new());
        }
    }

    fn recompute_after_append_leaves(&mut self, start_index: usize) {
        self.recompute(start_index, 0, None)
    }

    fn recompute_after_append_subtree(&mut self, start_index: usize, height: usize) {
        self.recompute(start_index, height, None)
    }

    fn recompute_after_fill_leaves(&mut self, start_index: usize, end_index: usize) {
        self.recompute(start_index, 0, Some(end_index))
    }

    /// Given a range of changed leaf nodes and recompute the tree.
    /// Since this tree is append-only, we always compute to the end.
    fn recompute(
        &mut self,
        mut start_index: usize,
        mut height: usize,
        mut maybe_end_index: Option<usize>,
    ) {
        start_index >>= height;
        maybe_end_index = maybe_end_index.map(|end| end >> height);
        // Loop until we compute the new root and reach `tree_depth`.
        while self.layers[height].len() > 1 || height < self.layers.len() - 1 {
            let next_layer_start_index = start_index >> 1;
            if start_index % 2 == 1 {
                start_index -= 1;
            }

            let mut end_index = maybe_end_index.unwrap_or(self.layers[height].len());
            if end_index % 2 == 1 && end_index != self.layers[height].len() {
                end_index += 1;
            }
            let mut i = 0;
            let mut iter = self.layers[height][start_index..end_index].chunks_exact(2);
            // We cannot modify the parent layer while iterating the child layer,
            // so just keep the changes and update them later.
            let mut parent_update = Vec::new();
            while let Some([left, right]) = iter.next() {
                // If either left or right is null (unknown), we cannot compute the parent hash.
                // Note that if we are recompute a range of an existing tree,
                // we do not need to keep these possibly null parent. This is only saved
                // for the case of constructing a new tree from the leaves.
                let parent = if *left == E::null() || *right == E::null() {
                    E::null()
                } else {
                    A::parent(left, right)
                };
                parent_update.push((next_layer_start_index + i, parent));
                i += 1;
            }
            if let [r] = iter.remainder() {
                // Same as above.
                let parent = if *r == E::null() {
                    E::null()
                } else {
                    A::parent_single(r, height + self.leaf_height)
                };
                parent_update.push((next_layer_start_index + i, parent));
            }
            if !parent_update.is_empty() {
                self.before_extend_layer(height + 1);
            }
            // `parent_update` is in increasing order by `parent_index`, so
            // we can just overwrite `last_changed_parent_index` with new values.
            let mut last_changed_parent_index = None;
            for (parent_index, parent) in parent_update {
                match parent_index.cmp(&self.layers[height + 1].len()) {
                    Ordering::Less => {
                        // We do not overwrite with null.
                        if parent != E::null() {
                            if self.layers[height + 1][parent_index] == E::null()
                                // The last node in a layer can be updated.
                                || (self.layers[height + 1][parent_index] != parent
                                    && parent_index == self.layers[height + 1].len() - 1)
                            {
                                self.layers[height + 1][parent_index] = parent;
                                last_changed_parent_index = Some(parent_index);
                            } else if self.layers[height + 1][parent_index] != parent {
                                // Recompute changes a node in the middle. This should be impossible
                                // if the inputs are valid.
                                panic!("Invalid append merkle tree! height={} index={} expected={:?} get={:?}",
                                       height + 1, parent_index, self.layers[height + 1][parent_index], parent);
                            }
                        }
                    }
                    Ordering::Equal => {
                        self.layers[height + 1].push(parent);
                        last_changed_parent_index = Some(parent_index);
                    }
                    Ordering::Greater => {
                        unreachable!("depth={}, parent_index={}", height, parent_index);
                    }
                }
            }
            if last_changed_parent_index.is_none() {
                break;
            }
            maybe_end_index = last_changed_parent_index.map(|i| i + 1);
            height += 1;
            start_index = next_layer_start_index;
        }
    }

    fn append_subtree_inner(&mut self, subtree_depth: usize, subtree_root: E) -> Result<()> {
        if subtree_depth == 0 {
            bail!("Subtree depth should not be zero!");
        }
        if self.leaves() % (1 << (subtree_depth - 1)) != 0 {
            warn!(
                "The current leaves count is not aligned with the merged subtree, \
                this is only possible during recovery, leaves={}",
                self.leaves()
            );
        }
        for height in 0..(subtree_depth - 1) {
            self.before_extend_layer(height);
            let subtree_layer_size = 1 << (subtree_depth - 1 - height);
            self.layers[height].append(&mut vec![E::null(); subtree_layer_size]);
        }
        self.before_extend_layer(subtree_depth - 1);
        self.layers[subtree_depth - 1].push(subtree_root);
        Ok(())
    }

    #[cfg(test)]
    pub fn validate(&self, proof: &Proof<E>, leaf: &E, position: usize) -> Result<bool> {
        proof.validate::<A>(leaf, position)?;
        Ok(self.root_to_tx_seq_map.contains_key(&proof.root()))
    }

    pub fn revert_to(&mut self, tx_seq: u64) -> Result<()> {
        if self.layers[0].is_empty() {
            // Any previous state of an empty tree is always empty.
            return Ok(());
        }
        let delta_nodes = self
            .delta_nodes_map
            .get(&tx_seq)
            .ok_or_else(|| anyhow!("tx_seq unavailable, root={:?}", tx_seq))?;
        // Dropping the upper layers that are not in the old merkle tree.
        self.layers.truncate(delta_nodes.right_most_nodes.len());
        for (height, (last_index, right_most_node)) in
            delta_nodes.right_most_nodes.iter().enumerate()
        {
            self.layers[height].truncate(*last_index + 1);
            self.layers[height][*last_index] = right_most_node.clone();
        }
        self.clear_after(tx_seq);
        Ok(())
    }

    pub fn tx_seq_at_root(&self, root_hash: &E) -> Result<u64> {
        self.root_to_tx_seq_map
            .get(root_hash)
            .cloned()
            .ok_or_else(|| anyhow!("old root unavailable, root={:?}", root_hash))
    }

    pub fn at_version(&self, tx_seq: u64) -> Result<HistoryTree<E>> {
        let delta_nodes = self
            .delta_nodes_map
            .get(&tx_seq)
            .ok_or_else(|| anyhow!("tx_seq unavailable, tx_seq={:?}", tx_seq))?;
        if delta_nodes.height() == 0 {
            bail!("empty tree");
        }
        Ok(HistoryTree {
            layers: &self.layers,
            delta_nodes,
            leaf_height: self.leaf_height,
        })
    }

    pub fn reset(&mut self) {
        self.layers = match self.min_depth {
            None => vec![vec![]],
            Some(depth) => vec![vec![]; depth],
        };
    }

    fn clear_after(&mut self, tx_seq: u64) {
        let mut tx_seq = tx_seq + 1;
        while self.delta_nodes_map.contains_key(&tx_seq) {
            if let Some(nodes) = self.delta_nodes_map.remove(&tx_seq) {
                if nodes.height() != 0 {
                    self.root_to_tx_seq_map.remove(nodes.root());
                }
            }
            tx_seq += 1;
        }
    }

    /// Return the height and the root hash of the first available node from the leaf to the root.
    /// The caller should ensure that `index` is within range.
    fn first_known_root_at(&self, index: usize) -> (usize, E) {
        let mut height = 0;
        let mut index_in_layer = index;
        while height < self.layers.len() {
            let node = self.node(height, index_in_layer);
            if !node.is_null() {
                return (height + 1, node.clone());
            }
            height += 1;
            index_in_layer /= 2;
        }
        unreachable!("root is always available")
    }
}

#[derive(Clone, Debug)]
struct DeltaNodes<E: HashElement> {
    /// The right most nodes in a layer and its position.
    right_most_nodes: Vec<(usize, E)>,
}

impl<E: HashElement> DeltaNodes<E> {
    fn new(right_most_nodes: Vec<(usize, E)>) -> Self {
        Self { right_most_nodes }
    }

    fn get(&self, height: usize, position: usize) -> Result<Option<&E>> {
        if height >= self.right_most_nodes.len() || position > self.right_most_nodes[height].0 {
            Err(anyhow!("position out of tree range"))
        } else if position == self.right_most_nodes[height].0 {
            Ok(Some(&self.right_most_nodes[height].1))
        } else {
            Ok(None)
        }
    }

    fn layer_len(&self, height: usize) -> usize {
        self.right_most_nodes[height].0 + 1
    }

    fn height(&self) -> usize {
        self.right_most_nodes.len()
    }

    fn root(&self) -> &E {
        &self.right_most_nodes.last().unwrap().1
    }
}

pub struct HistoryTree<'m, E: HashElement> {
    /// A reference to the global tree nodes.
    layers: &'m Vec<Vec<E>>,
    /// The delta nodes that are difference from `layers`.
    /// This could be a reference, we just take ownership for convenience.
    delta_nodes: &'m DeltaNodes<E>,

    leaf_height: usize,
}

impl<E: HashElement, A: Algorithm<E>> MerkleTreeRead for AppendMerkleTree<E, A> {
    type E = E;

    fn node(&self, layer: usize, index: usize) -> &Self::E {
        &self.layers[layer][index]
    }

    fn height(&self) -> usize {
        self.layers.len()
    }

    fn layer_len(&self, layer_height: usize) -> usize {
        self.layers[layer_height].len()
    }

    fn padding_node(&self, height: usize) -> Self::E {
        E::end_pad(height + self.leaf_height)
    }
}

impl<'a, E: HashElement> MerkleTreeRead for HistoryTree<'a, E> {
    type E = E;
    fn node(&self, layer: usize, index: usize) -> &Self::E {
        match self.delta_nodes.get(layer, index).expect("range checked") {
            Some(node) if *node != E::null() => node,
            _ => &self.layers[layer][index],
        }
    }

    fn height(&self) -> usize {
        self.delta_nodes.height()
    }

    fn layer_len(&self, layer_height: usize) -> usize {
        self.delta_nodes.layer_len(layer_height)
    }

    fn padding_node(&self, height: usize) -> Self::E {
        E::end_pad(height + self.leaf_height)
    }
}

#[macro_export]
macro_rules! ensure_eq {
    ($given:expr, $expected:expr) => {
        ensure!(
            $given == $expected,
            format!(
                "equal check fails! {}:{}: {}={:?}, {}={:?}",
                file!(),
                line!(),
                stringify!($given),
                $given,
                stringify!($expected),
                $expected,
            )
        );
    };
}

#[cfg(test)]
mod tests {
    use crate::merkle_tree::MerkleTreeRead;
    use crate::sha3::Sha3Algorithm;
    use crate::AppendMerkleTree;
    use ethereum_types::H256;

    #[test]
    fn test_proof() {
        let n = [1, 2, 6, 1025];
        for entry_len in n {
            let mut data = Vec::new();
            for _ in 0..entry_len {
                data.push(H256::random());
            }
            let mut merkle =
                AppendMerkleTree::<H256, Sha3Algorithm>::new(vec![H256::zero()], 0, None);
            merkle.append_list(data.clone());
            merkle.commit(Some(0));
            verify(&data, &mut merkle);

            data.push(H256::random());
            merkle.append(*data.last().unwrap());
            merkle.commit(Some(1));
            verify(&data, &mut merkle);

            for _ in 0..6 {
                data.push(H256::random());
            }
            merkle.append_list(data[data.len() - 6..].to_vec());
            merkle.commit(Some(2));
            verify(&data, &mut merkle);
        }
    }

    #[test]
    fn test_proof_against_modified_merkle() {
        let n = [1, 2, 6, 1025];
        for entry_len in n {
            let mut data = Vec::new();
            for _ in 0..entry_len {
                data.push(H256::random());
            }
            let mut merkle =
                AppendMerkleTree::<H256, Sha3Algorithm>::new(vec![H256::zero()], 0, None);
            merkle.append_list(data.clone());
            merkle.commit(Some(0));

            for i in (0..data.len()).step_by(6) {
                let end = std::cmp::min(i + 3, data.len());
                let range_proof = merkle.gen_range_proof(i + 1, end + 1).unwrap();
                let mut new_data = Vec::new();
                for _ in 0..3 {
                    new_data.push(H256::random());
                }
                merkle.append_list(new_data);
                let seq = i as u64 / 6 + 1;
                merkle.commit(Some(seq));
                let r = range_proof.validate::<Sha3Algorithm>(&data[i..end], i + 1);
                assert!(r.is_ok(), "{:?}", r);
                merkle.fill_with_range_proof(range_proof).unwrap();
            }
        }
    }

    #[test]
    fn test_proof_at_version() {
        let n = [2, 255, 256, 257];
        let mut merkle = AppendMerkleTree::<H256, Sha3Algorithm>::new(vec![H256::zero()], 0, None);
        let mut start_pos = 0;

        for (tx_seq, &entry_len) in n.iter().enumerate() {
            let mut data = Vec::new();
            for _ in 0..entry_len {
                data.push(H256::random());
            }
            merkle.append_list(data.clone());
            merkle.commit(Some(tx_seq as u64));
            for i in (0..data.len()).step_by(6) {
                let end = std::cmp::min(start_pos + i + 3, data.len());
                let range_proof = merkle
                    .at_version(tx_seq as u64)
                    .unwrap()
                    .gen_range_proof(start_pos + i + 1, start_pos + end + 1)
                    .unwrap();
                let r = range_proof.validate::<Sha3Algorithm>(&data[i..end], start_pos + i + 1);
                assert!(r.is_ok(), "{:?}", r);
                merkle.fill_with_range_proof(range_proof).unwrap();
            }

            start_pos += entry_len;
        }
    }

    fn verify(data: &[H256], merkle: &mut AppendMerkleTree<H256, Sha3Algorithm>) {
        for (i, item) in data.iter().enumerate() {
            let proof = merkle.gen_proof(i + 1).unwrap();
            let r = merkle.validate(&proof, item, i + 1);
            assert!(matches!(r, Ok(true)), "{:?}", r);
        }
        for i in (0..data.len()).step_by(6) {
            let end = std::cmp::min(i + 3, data.len());
            let range_proof = merkle.gen_range_proof(i + 1, end + 1).unwrap();
            let r = range_proof.validate::<Sha3Algorithm>(&data[i..end], i + 1);
            assert!(r.is_ok(), "{:?}", r);
            merkle.fill_with_range_proof(range_proof).unwrap();
        }
    }
}
