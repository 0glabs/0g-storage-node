use crate::{Algorithm, HashElement};
use tiny_keccak::{Hasher, Keccak};

pub struct Sha3Algorithm {}

impl<E: HashElement> Algorithm<E> for Sha3Algorithm {
    fn parent(left: &E, right: &E) -> E {
        let mut h = Keccak::v256();
        let mut e = E::null();
        h.update(left.as_ref());
        h.update(right.as_ref());
        h.finalize(e.as_mut());
        e
    }

    fn leaf(data: &[u8]) -> E {
        let mut h = Keccak::v256();
        let mut e = E::null();
        h.update(data.as_ref());
        h.finalize(e.as_mut());
        e
    }
}
