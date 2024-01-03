use merkle_light::hash::Algorithm;
use std::hash::Hasher;
use tiny_keccak::{Hasher as KeccakHasher, Keccak};

// TODO: Option here is only used for compatibility with `tiny_keccak` and `merkle_light`.
#[derive(Clone)]
pub struct RawLeafSha3Algorithm(Option<Keccak>);

impl RawLeafSha3Algorithm {
    fn new() -> RawLeafSha3Algorithm {
        RawLeafSha3Algorithm(Some(Keccak::v256()))
    }
}

impl Default for RawLeafSha3Algorithm {
    fn default() -> RawLeafSha3Algorithm {
        RawLeafSha3Algorithm::new()
    }
}

impl Hasher for RawLeafSha3Algorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.as_mut().unwrap().update(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

pub type CryptoSHA256Hash = [u8; 32];

impl Algorithm<CryptoSHA256Hash> for RawLeafSha3Algorithm {
    #[inline]
    fn hash(&mut self) -> CryptoSHA256Hash {
        let mut h = [0u8; 32];
        self.0.take().unwrap().finalize(&mut h);
        h
    }

    fn leaf(&mut self, leaf: CryptoSHA256Hash) -> CryptoSHA256Hash {
        // Leave the leaf node untouched so we can save the subtree root
        // just as the leaf node for the top tree.
        // `LEAF` is prepended for `Chunk` hash computation.
        leaf
    }

    #[inline]
    fn node(&mut self, left: CryptoSHA256Hash, right: CryptoSHA256Hash) -> CryptoSHA256Hash {
        self.write(left.as_ref());
        self.write(right.as_ref());
        self.hash()
    }
}

#[cfg(test)]
mod tests {
    use crate::RawLeafSha3Algorithm;
    use merkle_light::{hash::Algorithm, merkle::MerkleTree};
    use std::hash::Hasher;

    #[test]
    fn test_root() {
        let results = [
            [
                86, 124, 71, 168, 121, 121, 77, 212, 137, 162, 16, 222, 193, 125, 49, 204, 89, 25,
                188, 66, 125, 19, 141, 113, 106, 129, 7, 224, 37, 226, 219, 203,
            ],
            [
                41, 66, 83, 171, 49, 203, 249, 13, 187, 190, 247, 85, 167, 95, 241, 96, 29, 167,
                144, 227, 92, 54, 95, 83, 14, 124, 26, 28, 169, 4, 220, 248,
            ],
        ];
        for (test_index, n_chunk) in [6, 7].into_iter().enumerate() {
            let mut data = Vec::with_capacity(n_chunk);
            for _ in 0..n_chunk {
                let mut a = RawLeafSha3Algorithm::default();
                a.write(&[0; 256]);
                data.push(a.hash());
            }
            let mt = MerkleTree::<_, RawLeafSha3Algorithm>::new(data);
            println!("{:?} {}", mt.root(), hex::encode(mt.root()));
            assert_eq!(results[test_index], mt.root());
        }
    }
}
