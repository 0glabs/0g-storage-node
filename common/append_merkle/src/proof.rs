use crate::{ensure_eq, Algorithm, HashElement};
use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};

#[derive(Clone, Debug, Eq, PartialEq, DeriveEncode, DeriveDecode, Deserialize, Serialize)]
pub struct Proof<T: HashElement> {
    lemma: Vec<T>,
    path: Vec<bool>,
}

impl<T: HashElement> Proof<T> {
    /// Creates new MT inclusion proof
    pub fn new(hash: Vec<T>, path: Vec<bool>) -> Proof<T> {
        assert_eq!(hash.len() - 2, path.len());
        Proof { lemma: hash, path }
    }

    pub fn new_empty() -> Proof<T> {
        Proof {
            lemma: vec![],
            path: vec![],
        }
    }

    /// Return proof target leaf
    pub fn item(&self) -> T {
        self.lemma.first().unwrap().clone()
    }

    /// Return tree root
    pub fn root(&self) -> T {
        self.lemma.last().unwrap().clone()
    }

    /// Verifies MT inclusion proof
    fn validate_integrity<A: Algorithm<T>>(&self) -> bool {
        let size = self.lemma.len();

        if size < 2 {
            return false;
        }
        let mut h = self.item();

        for i in 1..size - 1 {
            h = if self.path[i - 1] {
                A::parent(&h, &self.lemma[i])
            } else {
                A::parent(&self.lemma[i], &h)
            };
        }

        h == self.root()
    }

    pub fn validate<A: Algorithm<T>>(&self, item: &T, position: usize) -> Result<()> {
        if !self.validate_integrity::<A>() {
            bail!("Invalid proof");
        }
        if *item != self.item() {
            bail!("Proof item unmatch");
        }
        if position != self.position() {
            bail!("Proof position unmatch");
        }
        Ok(())
    }

    /// Returns the path of this proof.
    pub fn path(&self) -> &[bool] {
        &self.path
    }

    /// Returns the lemma of this proof.
    pub fn lemma(&self) -> &[T] {
        &self.lemma
    }

    pub fn position(&self) -> usize {
        let mut pos = 0;
        for (i, is_left) in self.path.iter().enumerate() {
            if !is_left {
                pos += 1 << i;
            }
        }
        pos
    }
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveEncode, DeriveDecode, Deserialize, Serialize)]
pub struct RangeProof<E: HashElement> {
    pub left_proof: Proof<E>,
    pub right_proof: Proof<E>,
}

impl<E: HashElement> RangeProof<E> {
    pub fn new_empty() -> Self {
        Self {
            left_proof: Proof::new_empty(),
            right_proof: Proof::new_empty(),
        }
    }

    fn validate_integrity<A: Algorithm<E>>(&self) -> bool {
        self.left_proof.validate_integrity::<A>()
            && self.right_proof.validate_integrity::<A>()
            && self.left_proof.root() == self.right_proof.root()
            && self.left_proof.path().len() == self.right_proof.path().len()
    }

    pub fn root(&self) -> E {
        self.left_proof.root()
    }

    pub fn validate<A: Algorithm<E>>(
        &self,
        range_leaves: &[E],
        start_position: usize,
    ) -> Result<()> {
        if !self.validate_integrity::<A>() {
            bail!("Invalid range proof");
        }
        if range_leaves.is_empty() {
            bail!("Empty range");
        }
        let end_position = start_position + range_leaves.len() - 1;
        ensure_eq!(self.left_proof.item(), range_leaves[0]);
        ensure_eq!(
            self.right_proof.item(),
            *range_leaves.last().expect("not empty")
        );
        ensure_eq!(self.left_proof.position(), start_position);
        ensure_eq!(self.right_proof.position(), end_position);
        let tree_depth = self.left_proof.path().len() + 1;
        // TODO: We can avoid copying the first layer.
        let mut children_layer = range_leaves.to_vec();
        for height in 0..(tree_depth - 1) {
            let mut parent_layer = Vec::new();
            let start_index = if !self.left_proof.path()[height] {
                // If the left-most node is the right child, its sibling is not within the data range and should be retrieved from the proof.
                let parent = A::parent(&self.left_proof.lemma()[height + 1], &children_layer[0]);
                parent_layer.push(parent);
                1
            } else {
                // The left-most node is the left child, its sibling is just the next child.
                0
            };
            let mut iter = children_layer[start_index..].chunks_exact(2);
            while let Some([left, right]) = iter.next() {
                parent_layer.push(A::parent(left, right))
            }
            if let [right_most] = iter.remainder() {
                if self.right_proof.path()[height] {
                    parent_layer.push(A::parent(right_most, &self.right_proof.lemma()[height + 1]));
                } else {
                    bail!("Unexpected error");
                }
            }
            children_layer = parent_layer;
        }
        assert_eq!(children_layer.len(), 1);
        let computed_root = children_layer.pop().unwrap();
        ensure_eq!(computed_root, self.root());

        Ok(())
    }
}
