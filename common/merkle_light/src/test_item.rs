#![cfg(test)]
#![allow(unsafe_code)]

use crate::hash::{Algorithm, Hashable};
use std::mem;
use std::slice;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Debug)]
pub struct Item(pub u64);

impl AsRef<[u8]> for Item {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(mem::transmute(&self.0), 8) }
    }
}

impl PartialEq<u64> for Item {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl From<u64> for Item {
    fn from(x: u64) -> Self {
        Item(x)
    }
}

impl From<Item> for u64 {
    fn from(val: Item) -> Self {
        val.0
    }
}

impl<A: Algorithm<Item>> Hashable<A> for Item {
    fn hash(&self, state: &mut A) {
        state.write_u64(self.0)
    }
}
