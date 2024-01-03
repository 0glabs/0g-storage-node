//! light _Merkle Tree_ implementation.
//!
//! Merkle tree (MT) implemented as a full binary tree allocated as a vec
//! of statically sized hashes to give hashes more locality. MT specialized
//! to the extent of hashing algorithm and hash item. [`Hashable`] trait is
//! compatible to the `std::hash::Hasher` and supports custom hash algorithms.
//! Implementation does not depend on any external crypto libraries, and tries
//! to be as performant as possible.
//!
//! This tree implementation uses encoding scheme as in _Certificate Transparency_
//! by default. Encoding scheme for leafs and nodes can be overridden though.
//! [RFC 6962](https://tools.ietf.org/html/rfc6962):
//!
//! ```text
//! MTH({d(0)}) = ALG(0x00 || d(0)).
//! For n > 1, let k be the largest power of two smaller than n (i.e.,
//! k < n <= 2k).  The Merkle tree Hash of an n-element list D[n] is then
//! defined recursively as
//! MTH(D[n]) = ALG(0x01 || MTH(D[0:k]) || MTH(D[k:n])),
//! ```
//!
//! Link: [](https://en.wikipedia.org/wiki/Merkle_tree)
//!
//! # Implementation choices
//!
//! Main idea is the whole code must obtain specialization at compile time with
//! minimum allocations calls, hashes must be of fixed size arrays known at
//! compile time, hash algorithm must be a trait and must not depend on any
//! external cryptographic libraries and the lib itself must somehow mimic std Rust api.
//!
//! Standard way in Rust is to hash objects with a `std::hash::Hasher`, and mainly
//! that is the reason behind the choice of the abstractions:
//!
//! `Object : Hashable<H> -> Hasher + Algorithm <- Merkle Tree`
//!
//! Custom [`merkle::hash::Hashable`] trait allows implementations differ
//! from [`std::collection`] related hashes, different implementations for
//! different hashing algorithms / schemas and conforms object-safety trait rules.
//!
//! [`Algorithm`] complements [`Hasher`] to be reusable and follows the idea
//! that the result hash is a mapping of the data stream.
//!
//! [`Algorithm.hash`] had to change its signature to be `&mut self` (`&self`) because
//! most of the cryptographic digest algorithms breaks current state on finalization
//! into unusable. `ring` libra tho contains interfaces incompatible to
//! `start-update-finish-reset` lifecycle. It requires either `cloning()` its state
//! on finalization, or `Cell`-ing via unsafe.
//!
//! Turning back to having [`Algorithm.write(&mut self, &[u8])`] instead of
//! `write(T)` allows to relax [`Algorithm`] trait [`Hasher`] constraint, even tho
//! works together well still.
//!
//! # Interface
//!
//! ```text
//! - build_tree (items) -> tree
//! - get_root -> hash
//! - gen_proof -> proof
//! - validate_proof (proof, leaf, root) -> bool
//! ```
//!
//! # Examples
//!
//! [`test_cmh.rs`]: custom merkle hasher implementation example
//! [`crypto_bitcoin_mt.rs`]: bitcoin merkle tree using crypto lib
//! [`crypto_chaincore_mt.rs`]: chain core merkle tree using crypto lib
//! [`ring_bitcoin_mt.rs`]: bitcoin merkle tree using ring lib
//!
//! # Quick start
//!
//! ```
//! #[cfg(feature = "chaincore")]
//! extern crate crypto;
//! extern crate merkle_light;
//!
//! #[cfg(feature = "chaincore")]
//! mod example {
//!     use std::fmt;
//!     use std::hash::Hasher;
//!     use std::iter::FromIterator;
//!     use crypto::sha3::{Sha3, Sha3Mode};
//!     use crypto::digest::Digest;
//!     use merkle_light::hash::{Algorithm, Hashable};
//!
//!     pub struct ExampleAlgorithm(Sha3);
//!
//!     impl ExampleAlgorithm {
//!         pub fn new() -> ExampleAlgorithm {
//!             ExampleAlgorithm(Sha3::new(Sha3Mode::Sha3_256))
//!         }
//!     }
//!
//!     impl Default for ExampleAlgorithm {
//!         fn default() -> ExampleAlgorithm {
//!             ExampleAlgorithm::new()
//!         }
//!     }
//!
//!     impl Hasher for ExampleAlgorithm {
//!         #[inline]
//!         fn write(&mut self, msg: &[u8]) {
//!             self.0.input(msg)
//!         }
//!
//!         #[inline]
//!         fn finish(&self) -> u64 {
//!             unimplemented!()
//!         }
//!     }
//!
//!     impl Algorithm<[u8; 32]> for ExampleAlgorithm {
//!         #[inline]
//!         fn hash(&mut self) -> [u8; 32] {
//!             let mut h = [0u8; 32];
//!             self.0.result(&mut h);
//!             h
//!         }
//!
//!         #[inline]
//!         fn reset(&mut self) {
//!             self.0.reset();
//!         }
//!     }
//! }
//!
//! fn main() {
//! #[cfg(feature = "chaincore")]
//! {
//!     use example::ExampleAlgorithm;
//!     use merkle_light::merkle::MerkleTree;
//!     use std::iter::FromIterator;
//!
//!     let mut h1 = [0u8; 32];
//!     let mut h2 = [0u8; 32];
//!     let mut h3 = [0u8; 32];
//!     h1[0] = 0x11;
//!     h2[0] = 0x22;
//!     h3[0] = 0x33;
//!
//!     let t: MerkleTree<[u8; 32], ExampleAlgorithm> = MerkleTree::from_iter(vec![h1, h2, h3]);
//!     println!("{:?}", t.root());
//! }
//! }
//! ```

#![deny(
    missing_docs,
    unused_qualifications,
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces
)]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(feature = "nightly", allow(unstable_features))]

/// Hash infrastructure for items in Merkle tree.
pub mod hash;

/// Common implementations for [`Hashable`].
mod hash_impl;

/// Merkle tree inclusion proof
pub mod proof;

/// Merkle tree abstractions, implementation and algorithms.
pub mod merkle;

/// Tests data.
#[cfg(test)]
mod test_item;

/// Tests for Merkle Hasher Customization
#[cfg(test)]
mod test_cmh;
