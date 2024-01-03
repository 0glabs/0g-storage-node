# merkle

[![Build Status](https://travis-ci.org/sitano/merkle_light.svg?branch=master&style=flat)](https://travis-ci.org/sitano/merkle_light)
[![Issues](http://img.shields.io/github/issues/sitano/merkle.svg?style=flat)](https://github.com/sitano/merkle_light/issues)
![License](https://img.shields.io/badge/license-bsd3-brightgreen.svg?style=flat)
[![Crates.io](https://img.shields.io/crates/v/merkle_light.svg)](https://crates.io/crates/merkle_light)

*merkle* is a lightweight Rust implementation of a [Merkle tree](https://en.wikipedia.org/wiki/Merkle_tree).

## Features

- external dependency agnostic
- `core::hash::Hasher` compatibility
- standard types hasher implementations
- `#[derive(Hashable)]` support for simple struct
- customizable merkle leaf/node hashing algorithm
- support for custom hash types (e.g. [u8; 16], [u64; 4], [u128; 2], struct)
- customizable hashing algorithm
- linear memory layout, no nodes on heap
- buildable from iterator, objects or hashes
- certificate transparency style merkle hashing support
- SPV included

## Documentation

Documentation is [available](https://sitano.github.io/merkle_light/merkle_light/index.html).

# Examples

* `test_sip.rs`: algorithm implementation example for std sip hasher, u64 hash items
* `test_xor128.rs`: custom hash example xor128
* `test_cmh.rs`: custom merkle hasher implementation example
* `crypto_bitcoin_mt.rs`: bitcoin merkle tree using crypto lib
* `crypto_chaincore_mt.rs`: chain core merkle tree using crypto lib
* `ring_bitcoin_mt.rs`: bitcoin merkle tree using ring lib

# Quick start

```
extern crate crypto;
extern crate merkle_light;

use std::fmt;
use std::hash::Hasher;
use std::iter::FromIterator;
use crypto::sha3::{Sha3, Sha3Mode};
use crypto::digest::Digest;
use merkle_light::hash::{Algorithm, Hashable};
use merkle_light::merkle::MerkleTree;

pub struct ExampleAlgorithm(Sha3);

impl ExampleAlgorithm {
    pub fn new() -> ExampleAlgorithm {
        ExampleAlgorithm(Sha3::new(Sha3Mode::Sha3_256))
    }
}

impl Default for ExampleAlgorithm {
    fn default() -> ExampleAlgorithm {
        ExampleAlgorithm::new()
    }
}

impl Hasher for ExampleAlgorithm {
    #[inline]
    fn write(&mut self, msg: &[u8]) {
        self.0.input(msg)
    }

    #[inline]
    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

impl Algorithm<[u8; 32]> for ExampleAlgorithm {
    #[inline]
    fn hash(&mut self) -> [u8; 32] {
        let mut h = [0u8; 32];
        self.0.result(&mut h);
        h
    }

    #[inline]
    fn reset(&mut self) {
        self.0.reset();
    }
}

fn main() {
    let mut h1 = [0u8; 32];
    let mut h2 = [0u8; 32];
    let mut h3 = [0u8; 32];
    h1[0] = 0x11;
    h2[0] = 0x22;
    h3[0] = 0x33;

    let t: MerkleTree<[u8; 32], ExampleAlgorithm> = MerkleTree::from_iter(vec![h1, h2, h3]);
    println!("{:?}", t.root());
}
```

## Bug Reporting

Please report bugs either as pull requests or as issues in [the issue
tracker](https://github.com/sitano/merkle_light). *merkle* has a
**full disclosure** vulnerability policy. **Please do NOT attempt to report
any security vulnerability in this code privately to anybody.**

## License

See [LICENSE](LICENSE).
