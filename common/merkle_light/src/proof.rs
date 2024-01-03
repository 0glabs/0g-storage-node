extern crate alloc;

use crate::hash::Algorithm;
use alloc::vec::Vec;

/// Merkle tree inclusion proof for data element, for which item = Leaf(Hash(Data Item)).
///
/// Lemma layout:
///
/// ```text
/// [ item h1x h2y h3z ... root ]
/// ```
///
/// Proof validation is positioned hash against lemma path to match root hash.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Proof<T: Eq + Clone + AsRef<[u8]>> {
    lemma: Vec<T>,
    path: Vec<bool>,
}

impl<T: Eq + Clone + AsRef<[u8]>> Proof<T> {
    /// Creates new MT inclusion proof
    pub fn new(hash: Vec<T>, path: Vec<bool>) -> Proof<T> {
        if hash.len() > 2 {
            assert_eq!(hash.len() - 2, path.len());
        } else {
            assert_eq!(hash.len(), 1);
            assert_eq!(path, vec![]);
        }
        Proof { lemma: hash, path }
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
    pub fn validate<A: Algorithm<T>>(&self) -> bool {
        let size = self.lemma.len();

        // Special case for a single node.
        if size == 1 && self.path == vec![] {
            return true;
        }

        if size < 2 {
            return false;
        }

        let mut h = self.item();
        let mut a = A::default();

        for i in 1..size - 1 {
            a.reset();
            h = if self.path[i - 1] {
                a.node(h, self.lemma[i].clone())
            } else {
                a.node(self.lemma[i].clone(), h)
            };
        }

        h == self.root()
    }

    /// Returns the path of this proof.
    pub fn path(&self) -> &[bool] {
        &self.path
    }

    /// Returns the lemma of this proof.
    pub fn lemma(&self) -> &[T] {
        &self.lemma
    }
}
