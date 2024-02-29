use crate::merkle_tree::ZERO_HASHES;
use crate::{Algorithm, HashElement};
use ethereum_types::H256;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use tiny_keccak::{Hasher, Keccak};

pub struct Sha3Algorithm {}

static ZERO_HASHES_MAP: Lazy<BTreeMap<H256, H256>> = Lazy::new(|| {
    let mut map = BTreeMap::new();
    for i in 1..ZERO_HASHES.len() {
        map.insert(ZERO_HASHES[i - 1], ZERO_HASHES[i]);
    }
    map
});

impl Sha3Algorithm {
    pub fn parent_raw(left: &H256, right: &H256) -> H256 {
        let mut h = Keccak::v256();
        let mut e = H256::null();
        h.update(left.as_ref());
        h.update(right.as_ref());
        h.finalize(e.as_mut());
        e
    }

    pub fn leaf_raw(data: &[u8]) -> H256 {
        let mut h = Keccak::v256();
        let mut e = H256::null();
        h.update(data.as_ref());
        h.finalize(e.as_mut());
        e
    }
}
impl Algorithm<H256> for Sha3Algorithm {
    fn parent(left: &H256, right: &H256) -> H256 {
        if left == right {
            if let Some(v) = ZERO_HASHES_MAP.get(left) {
                return *v;
            }
        }
        Self::parent_raw(left, right)
    }

    fn leaf(data: &[u8]) -> H256 {
        if *data == [0u8; 256] {
            return ZERO_HASHES[0];
        }
        Self::leaf_raw(data)
    }
}
