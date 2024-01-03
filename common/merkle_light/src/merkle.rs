extern crate alloc;

use crate::hash::{Algorithm, Hashable};
use crate::proof::Proof;
use alloc::vec::Vec;
use core::iter::FromIterator;
use core::marker::PhantomData;
use core::ops;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fmt::Debug;

/// Merkle Tree.
///
/// All leafs and nodes are stored in a linear array (vec).
///
/// A merkle tree is a tree in which every non-leaf node is the hash of its
/// children nodes. A diagram depicting how it works:
///
/// ```text
///         root = h1234 = h(h12 + h34)
///        /                           \
///  h12 = h(h1 + h2)            h34 = h(h3 + h4)
///   /            \              /            \
/// h1 = h(tx1)  h2 = h(tx2)    h3 = h(tx3)  h4 = h(tx4)
/// ```
///
/// In memory layout:
///
/// ```text
///     [h1 h2 h3 h4 h12 h34 root]
/// ```
///
/// Merkle root is always the last element in the array.
///
/// The number of inputs is not always a power of two which results in a
/// balanced tree structure as above.  In that case, parent nodes with no
/// children are also zero and parent nodes with only a single left node
/// are calculated by concatenating the left node with itself before hashing.
/// Since this function uses nodes that are pointers to the hashes, empty nodes
/// will be nil.
///
/// TODO: Ord
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MerkleTree<T: Ord + Clone + AsRef<[u8]> + Sync + Send, A: Algorithm<T>> {
    data: Vec<T>,
    leafs: usize,
    height: usize,
    link_map: BTreeMap<usize, usize>,
    _a: PhantomData<A>,
}

impl<T: Ord + Clone + Debug + Default + AsRef<[u8]> + Sync + Send, A: Algorithm<T>>
    MerkleTree<T, A>
{
    /// Creates new merkle from a sequence of hashes.
    pub fn new<I: IntoIterator<Item = T>>(data: I) -> MerkleTree<T, A> {
        Self::from_iter(data)
    }

    /// Creates new merkle tree from a list of hashable objects.
    pub fn from_data<O: Hashable<A>, I: IntoIterator<Item = O>>(data: I) -> MerkleTree<T, A> {
        let mut a = A::default();
        Self::from_iter(data.into_iter().map(|x| {
            a.reset();
            x.hash(&mut a);
            a.hash()
        }))
    }

    fn build(&mut self) {
        let mut width = self.leafs;

        // build tree
        let mut layer_start: usize = 0;
        let mut layer_end: usize = width;
        while width > 1 {
            // if there is odd num of elements, fill in a NULL.
            if width & 1 == 1 {
                self.data.push(Self::null_node());
                width += 1;
                layer_end += 1;
            }

            let layer: Vec<_> = (layer_start..layer_end)
                .into_par_iter()
                .step_by(2)
                .map(|i| {
                    let mut a = A::default();
                    // If the right child is not NULL, the left child is ensured to be not NULL.
                    let mut link_map_update = None;
                    let h = if self.data[i + 1] != Self::null_node() {
                        a.node(self.data[i].clone(), self.data[i + 1].clone())
                    } else {
                        // If a child is NULL, the parent should be a linking node to the actual node hash.
                        let parent_index = (i - layer_start) / 2 + layer_end;
                        if self.data[i] == Self::null_node() {
                            // If both are NULL, the left child must be a linking node.
                            let linked_to = *self.link_map.get(&i).unwrap();
                            link_map_update = Some((parent_index, linked_to, Some(i)));
                            Self::null_node()
                        } else {
                            match self.link_map.get(&(i + 1)) {
                                // Right child is linked to a hash, so we just compute the parent hash.
                                Some(index) => {
                                    assert_ne!(self.data[*index], Self::null_node());
                                    a.node(self.data[i].clone(), self.data[*index].clone())
                                }
                                // Right child is NULL, so link the parent to the left child which has a hash stored.
                                None => {
                                    link_map_update = Some((parent_index, i, None));
                                    Self::null_node()
                                }
                            }
                        }
                    };
                    (h, link_map_update)
                })
                .collect();
            for (node, maybe_link_map_update) in layer {
                self.data.push(node);
                if let Some((from, to, maybe_remove)) = maybe_link_map_update {
                    self.link_map.insert(from, to);
                    if let Some(remove) = maybe_remove {
                        self.link_map.remove(&remove);
                    }
                }
            }

            layer_start = layer_end;
            width >>= 1;
            layer_end += width;
        }
    }

    /// Generate merkle tree inclusion proof for leaf `i`
    pub fn gen_proof(&self, i: usize) -> Proof<T> {
        if self.leafs == 1 {
            assert_eq!(i, 0);
            return Proof::new(vec![self.root()], vec![]);
        }

        assert!(i < self.leafs); // i in [0 .. self.leafs)

        let mut lemma: Vec<T> = Vec::with_capacity(self.height + 1); // path + root
        let mut path: Vec<bool> = Vec::with_capacity(self.height - 1); // path - 1

        let mut base = 0;
        let mut j = i;

        // level 1 width
        let mut width = self.leafs;
        if width & 1 == 1 {
            width += 1;
        }

        lemma.push(self.data[j].clone());
        while base + 1 < self.len() {
            let proof_hash_index = if j & 1 == 0 {
                // j is left
                let right_index = base + j + 1;
                if self.data[right_index] == Self::null_node() {
                    match self.link_map.get(&right_index) {
                        // A link node, so the proof uses the linked hash.
                        Some(index) => {
                            assert_ne!(self.data[*index], Self::null_node());
                            Some(*index)
                        }
                        // A NULL node, just skip.
                        None => None,
                    }
                } else {
                    Some(right_index)
                }
            } else {
                // j is right
                Some(base + j - 1)
            };
            if let Some(index) = proof_hash_index {
                lemma.push(self.data[index].clone());
                path.push(j & 1 == 0);
            }

            base += width;
            width >>= 1;
            if width & 1 == 1 {
                width += 1;
            }
            j >>= 1;
        }

        // root is final
        lemma.push(self.root());
        Proof::new(lemma, path)
    }

    /// Returns merkle root
    pub fn root(&self) -> T {
        self.data[self.data.len() - 1].clone()
    }

    /// Returns number of elements in the tree.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns height of the tree
    pub fn height(&self) -> usize {
        self.height
    }

    /// Returns original number of elements the tree was built upon.
    pub fn leafs(&self) -> usize {
        self.leafs
    }

    /// Extracts a slice containing the entire vector.
    ///
    /// Equivalent to `&s[..]`.
    pub fn as_slice(&self) -> &[T] {
        self
    }

    fn null_node() -> T {
        T::default()
    }
}

impl<T: Ord + Clone + Debug + Default + AsRef<[u8]> + Sync + Send, A: Algorithm<T>> FromIterator<T>
    for MerkleTree<T, A>
{
    /// Creates new merkle tree from an iterator over hashable objects.
    fn from_iter<I: IntoIterator<Item = T>>(into: I) -> Self {
        let iter = into.into_iter();
        let mut data: Vec<T> = match iter.size_hint().1 {
            Some(e) => {
                let pow = next_pow2(e);
                let size = 2 * pow - 1;
                Vec::with_capacity(size)
            }
            None => Vec::new(),
        };

        // leafs
        let mut a = A::default();
        for item in iter {
            a.reset();
            data.push(a.leaf(item));
        }

        let leafs = data.len();
        let pow = next_pow2(leafs);
        let size = 2 * pow - 1;

        // assert!(leafs > 1);

        let mut mt: MerkleTree<T, A> = MerkleTree {
            data,
            leafs,
            height: log2_pow2(size + 1),
            link_map: Default::default(),
            _a: PhantomData,
        };

        mt.build();
        mt
    }
}

impl<T: Ord + Clone + AsRef<[u8]> + Sync + Send, A: Algorithm<T>> ops::Deref for MerkleTree<T, A> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.data.deref()
    }
}

/// `next_pow2` returns next highest power of two from a given number if
/// it is not already a power of two.
///
/// [](http://locklessinc.com/articles/next_pow2/)
/// [](https://stackoverflow.com/questions/466204/rounding-up-to-next-power-of-2/466242#466242)
pub fn next_pow2(mut n: usize) -> usize {
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    #[cfg(target_pointer_width = "64")]
    {
        n |= n >> 32;
    }
    n + 1
}

/// find power of 2 of a number which is power of 2
pub fn log2_pow2(n: usize) -> usize {
    n.trailing_zeros() as usize
}
