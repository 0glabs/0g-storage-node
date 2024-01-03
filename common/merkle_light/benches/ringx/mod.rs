//! Improvement version of [`ring::digest::Context`].
//!
//! [`Context.finish`] now has `(&mut self)` instead of `(mut self)`.
//! [`Context`] acquired state `reset` thing.

#![cfg(feature = "crypto_bench")]
#![allow(dead_code)]

extern crate rand;
extern crate ring;
extern crate test;

mod init;

use std::fmt;

// XXX: Replace with `const fn` when `const fn` is stable:
// https://github.com/rust-lang/rust/issues/24111
#[cfg(target_endian = "little")]
macro_rules! u32x2 {
    ( $first:expr, $second:expr ) => {
        ((($second as u64) << 32) | ($first as u64))
    };
}

/// A context for multi-step (Init-Update-Finish) digest calculations.
///
/// C analog: `EVP_MD_CTX`.
pub struct Context {
    state: State,

    // Note that SHA-512 has a 128-bit input bit counter, but this
    // implementation only supports up to 2^64-1 input bits for all algorithms,
    // so a 64-bit counter is more than sufficient.
    completed_data_blocks: u64,

    // TODO: More explicitly force 64-bit alignment for |pending|.
    pending: [u8; MAX_BLOCK_LEN],
    num_pending: usize,

    /// The context's algorithm.
    pub algorithm: &'static Algorithm,
}

impl Context {
    /// Constructs a new context.
    ///
    /// C analogs: `EVP_DigestInit`, `EVP_DigestInit_ex`
    pub fn new(algorithm: &'static Algorithm) -> Context {
        init::init_once();

        Context {
            algorithm,
            state: algorithm.initial_state,
            completed_data_blocks: 0,
            pending: [0u8; MAX_BLOCK_LEN],
            num_pending: 0,
        }
    }

    /// Updates the digest with all the data in `data`. `update` may be called
    /// zero or more times until `finish` is called. It must not be called
    /// after `finish` has been called.
    ///
    /// C analog: `EVP_DigestUpdate`
    pub fn update(&mut self, data: &[u8]) {
        if data.len() < self.algorithm.block_len - self.num_pending {
            self.pending[self.num_pending..(self.num_pending + data.len())].copy_from_slice(data);
            self.num_pending += data.len();
            return;
        }

        let mut remaining = data;
        if self.num_pending > 0 {
            let to_copy = self.algorithm.block_len - self.num_pending;
            self.pending[self.num_pending..self.algorithm.block_len]
                .copy_from_slice(&data[..to_copy]);

            unsafe {
                (self.algorithm.block_data_order)(&mut self.state, self.pending.as_ptr(), 1);
            }
            self.completed_data_blocks = self.completed_data_blocks.checked_add(1).unwrap();

            remaining = &remaining[to_copy..];
            self.num_pending = 0;
        }

        let num_blocks = remaining.len() / self.algorithm.block_len;
        let num_to_save_for_later = remaining.len() % self.algorithm.block_len;
        if num_blocks > 0 {
            unsafe {
                (self.algorithm.block_data_order)(&mut self.state, remaining.as_ptr(), num_blocks);
            }
            self.completed_data_blocks = self
                .completed_data_blocks
                .checked_add(polyfill::slice::u64_from_usize(num_blocks))
                .unwrap();
        }
        if num_to_save_for_later > 0 {
            self.pending[..num_to_save_for_later]
                .copy_from_slice(&remaining[(remaining.len() - num_to_save_for_later)..]);
            self.num_pending = num_to_save_for_later;
        }
    }

    /// Finalizes the digest calculation and returns the digest value. `finish`
    /// consumes the context so it cannot be (mis-)used after `finish` has been
    /// called.
    ///
    /// C analogs: `EVP_DigestFinal`, `EVP_DigestFinal_ex`
    pub fn finish(&mut self) -> Digest {
        // We know |num_pending < self.algorithm.block_len|, because we would
        // have processed the block otherwise.

        let mut padding_pos = self.num_pending;
        self.pending[padding_pos] = 0x80;
        padding_pos += 1;

        if padding_pos > self.algorithm.block_len - self.algorithm.len_len {
            polyfill::slice::fill(&mut self.pending[padding_pos..self.algorithm.block_len], 0);
            unsafe {
                (self.algorithm.block_data_order)(&mut self.state, self.pending.as_ptr(), 1);
            }
            // We don't increase |self.completed_data_blocks| because the
            // padding isn't data, and so it isn't included in the data length.
            padding_pos = 0;
        }

        polyfill::slice::fill(
            &mut self.pending[padding_pos..(self.algorithm.block_len - 8)],
            0,
        );

        // Output the length, in bits, in big endian order.
        let mut completed_data_bits: u64 = self
            .completed_data_blocks
            .checked_mul(polyfill::slice::u64_from_usize(self.algorithm.block_len))
            .unwrap()
            .checked_add(polyfill::slice::u64_from_usize(self.num_pending))
            .unwrap()
            .checked_mul(8)
            .unwrap();

        for b in (&mut self.pending[(self.algorithm.block_len - 8)..self.algorithm.block_len])
            .into_iter()
            .rev()
        {
            *b = completed_data_bits as u8;
            completed_data_bits /= 0x100;
        }
        unsafe {
            (self.algorithm.block_data_order)(&mut self.state, self.pending.as_ptr(), 1);
        }

        Digest {
            algorithm: self.algorithm,
            value: (self.algorithm.format_output)(&self.state),
        }
    }

    /// The algorithm that this context is using.
    #[inline(always)]
    pub fn algorithm(&self) -> &'static Algorithm {
        self.algorithm
    }

    /// Reset context state.
    pub fn reset(&mut self) {
        self.state = self.algorithm.initial_state;
        self.pending = [0u8; MAX_BLOCK_LEN];
        self.completed_data_blocks = 0;
        self.num_pending = 0;
    }
}

// XXX: This should just be `#[derive(Clone)]` but that doesn't work because
// `[u8; 128]` doesn't implement `Clone`.
impl Clone for Context {
    fn clone(&self) -> Context {
        Context {
            state: self.state,
            pending: self.pending,
            completed_data_blocks: self.completed_data_blocks,
            num_pending: self.num_pending,
            algorithm: self.algorithm,
        }
    }
}

/// Returns the digest of `data` using the given digest algorithm.
///
/// C analog: `EVP_Digest`
///
/// # Examples:
///
/// ```
/// # #[cfg(feature = "use_heap")]
/// # fn main() {
/// use ring::{digest, test};
///
/// let expected_hex =
///     "09ca7e4eaa6e8ae9c7d261167129184883644d07dfba7cbfbc4c8a2e08360d5b";
/// let expected: Vec<u8> = test::from_hex(expected_hex).unwrap();
/// let actual = digest::digest(&digest::SHA256, b"hello, world");
///
/// assert_eq!(&expected, &actual.as_ref());
/// # }
///
/// # #[cfg(not(feature = "use_heap"))]
/// # fn main() { }
/// ```
pub fn digest(algorithm: &'static Algorithm, data: &[u8]) -> Digest {
    let mut ctx = Context::new(algorithm);
    ctx.update(data);
    ctx.finish()
}

/// A calculated digest value.
///
/// Use `as_ref` to get the value as a `&[u8]`.
#[derive(Clone, Copy)]
pub struct Digest {
    value: Output,
    algorithm: &'static Algorithm,
}

impl Digest {
    /// The algorithm that was used to calculate the digest value.
    #[inline(always)]
    pub fn algorithm(&self) -> &'static Algorithm {
        self.algorithm
    }
}

impl AsRef<[u8]> for Digest {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &(polyfill::slice::u64_as_u8(&self.value))[..self.algorithm.output_len]
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}:", self.algorithm)?;
        for byte in self.as_ref() {
            write!(fmt, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// A digest algorithm.
///
/// C analog: `EVP_MD`
pub struct Algorithm {
    /// C analog: `EVP_MD_size`
    pub output_len: usize,

    /// The size of the chaining value of the digest function, in bytes. For
    /// non-truncated algorithms (SHA-1, SHA-256, SHA-512), this is equal to
    /// `output_len`. For truncated algorithms (e.g. SHA-384, SHA-512/256),
    /// this is equal to the length before truncation. This is mostly helpful
    /// for determining the size of an HMAC key that is appropriate for the
    /// digest algorithm.
    pub chaining_len: usize,

    /// C analog: `EVP_MD_block_size`
    pub block_len: usize,

    /// The length of the length in the padding.
    pub len_len: usize,

    pub block_data_order: unsafe extern "C" fn(state: &mut State, data: *const u8, num: usize),
    pub format_output: fn(input: &State) -> Output,

    pub initial_state: State,

    pub id: AlgorithmID,
}

#[derive(Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum AlgorithmID {
    SHA256,
    SHA512,
    SHA512_256,
}

impl PartialEq for Algorithm {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Algorithm {}

impl fmt::Debug for Algorithm {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        // This would have to change if/when we add other algorithms with the
        // same lengths.
        let (n, suffix) =
            if self.output_len == SHA512_256_OUTPUT_LEN && self.block_len == SHA512_BLOCK_LEN {
                (512, "_256")
            } else if self.output_len == 20 {
                (1, "")
            } else {
                (self.output_len * 8, "")
            };
        write!(fmt, "SHA{}{}", n, suffix)
    }
}

/// SHA-256 as specified in [FIPS 180-4].
///
/// [FIPS 180-4]: http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
pub static SHA256: Algorithm = Algorithm {
    output_len: SHA256_OUTPUT_LEN,
    chaining_len: SHA256_OUTPUT_LEN,
    block_len: 512 / 8,
    len_len: 64 / 8,
    block_data_order: GFp_sha256_block_data_order,
    format_output: sha256_format_output,
    initial_state: [
        u32x2!(0x6a09e667u32, 0xbb67ae85u32),
        u32x2!(0x3c6ef372u32, 0xa54ff53au32),
        u32x2!(0x510e527fu32, 0x9b05688cu32),
        u32x2!(0x1f83d9abu32, 0x5be0cd19u32),
        0,
        0,
        0,
        0,
    ],
    id: AlgorithmID::SHA256,
};

/// SHA-512 as specified in [FIPS 180-4].
///
/// [FIPS 180-4]: http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
pub static SHA512: Algorithm = Algorithm {
    output_len: SHA512_OUTPUT_LEN,
    chaining_len: SHA512_OUTPUT_LEN,
    block_len: SHA512_BLOCK_LEN,
    len_len: SHA512_LEN_LEN,
    block_data_order: GFp_sha512_block_data_order,
    format_output: sha512_format_output,
    initial_state: [
        0x6a09e667f3bcc908,
        0xbb67ae8584caa73b,
        0x3c6ef372fe94f82b,
        0xa54ff53a5f1d36f1,
        0x510e527fade682d1,
        0x9b05688c2b3e6c1f,
        0x1f83d9abfb41bd6b,
        0x5be0cd19137e2179,
    ],
    id: AlgorithmID::SHA512,
};

/// SHA-512/256 as specified in [FIPS 180-4].
///
/// This is *not* the same as just truncating the output of SHA-512, as
/// SHA-512/256 has its own initial state distinct from SHA-512's initial
/// state.
///
/// [FIPS 180-4]: http://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
pub static SHA512_256: Algorithm = Algorithm {
    output_len: SHA512_256_OUTPUT_LEN,
    chaining_len: SHA512_OUTPUT_LEN,
    block_len: SHA512_BLOCK_LEN,
    len_len: SHA512_LEN_LEN,
    block_data_order: GFp_sha512_block_data_order,
    format_output: sha512_format_output,
    initial_state: [
        0x22312194fc2bf72c,
        0x9f555fa3c84c64c2,
        0x2393b86b6f53b151,
        0x963877195940eabd,
        0x96283ee2a88effe3,
        0xbe5e1e2553863992,
        0x2b0199fc2c85b8aa,
        0x0eb72ddc81c52ca2,
    ],
    id: AlgorithmID::SHA512_256,
};

// We use u64 to try to ensure 64-bit alignment/padding.
pub type State = [u64; MAX_CHAINING_LEN / 8];

pub type Output = [u64; MAX_OUTPUT_LEN / 8];

/// The maximum block length (`Algorithm::block_len`) of all the algorithms in
/// this module.
pub const MAX_BLOCK_LEN: usize = 1024 / 8;

/// The maximum output length (`Algorithm::output_len`) of all the algorithms
/// in this module.
pub const MAX_OUTPUT_LEN: usize = 512 / 8;

/// The maximum chaining length (`Algorithm::chaining_len`) of all the
/// algorithms in this module.
pub const MAX_CHAINING_LEN: usize = MAX_OUTPUT_LEN;

mod polyfill {
    pub mod slice {
        use std::slice::from_raw_parts;

        // https://internals.rust-lang.org/t/
        // safe-trasnsmute-for-slices-e-g-u64-u32-particularly-simd-types/2871
        #[inline(always)]
        pub fn u64_as_u32(src: &[u64]) -> &[u32] {
            unsafe { from_raw_parts(src.as_ptr() as *const u32, src.len() * 2) }
        }

        #[inline(always)]
        pub fn u64_from_usize(x: usize) -> u64 {
            x as u64
        }

        // https://internals.rust-lang.org/t/
        // stabilizing-basic-functions-on-arrays-and-slices/2868
        #[inline(always)]
        pub fn fill(dest: &mut [u8], value: u8) {
            for d in dest {
                *d = value;
            }
        }

        // https://internals.rust-lang.org/t/
        // safe-trasnsmute-for-slices-e-g-u64-u32-particularly-simd-types/2871
        #[inline(always)]
        pub fn u64_as_u8(src: &[u64]) -> &[u8] {
            unsafe { from_raw_parts(src.as_ptr() as *const u8, src.len() * 8) }
        }
    }
}

pub fn sha256_format_output(input: &State) -> Output {
    let input = &polyfill::slice::u64_as_u32(input)[..8];
    [
        u32x2!(input[0].to_be(), input[1].to_be()),
        u32x2!(input[2].to_be(), input[3].to_be()),
        u32x2!(input[4].to_be(), input[5].to_be()),
        u32x2!(input[6].to_be(), input[7].to_be()),
        0,
        0,
        0,
        0,
    ]
}

pub fn sha512_format_output(input: &State) -> Output {
    [
        input[0].to_be(),
        input[1].to_be(),
        input[2].to_be(),
        input[3].to_be(),
        input[4].to_be(),
        input[5].to_be(),
        input[6].to_be(),
        input[7].to_be(),
    ]
}

/// The length of the output of SHA-256, in bytes.
pub const SHA256_OUTPUT_LEN: usize = 256 / 8;

/// The length of the output of SHA-512, in bytes.
pub const SHA512_OUTPUT_LEN: usize = 512 / 8;

/// The length of the output of SHA-512/256, in bytes.
pub const SHA512_256_OUTPUT_LEN: usize = 256 / 8;

/// The length of a block for SHA-512-based algorithms, in bytes.
const SHA512_BLOCK_LEN: usize = 1024 / 8;

/// The length of the length field for SHA-512-based algorithms, in bytes.
const SHA512_LEN_LEN: usize = 128 / 8;

extern "C" {
    fn GFp_sha256_block_data_order(state: &mut State, data: *const u8, num: usize);
    fn GFp_sha512_block_data_order(state: &mut State, data: *const u8, num: usize);
}

#[cfg(test)]
pub mod test_util {
    use super::*;

    pub static ALL_ALGORITHMS: [&'static Algorithm; 3] = [&SHA256, &SHA512, &SHA512_256];
}

/*
#[cfg(test)]
mod tests {

    mod max_input {
        use super::super::super::digest;

        macro_rules! max_input_tests {
            ( $algorithm_name:ident ) => {
                #[allow(non_snake_case)]
                mod $algorithm_name {
                    use super::super::super::super::digest;

                    #[test]
                    fn max_input_test() {
                        super::max_input_test(&digest::$algorithm_name);
                    }

                    #[test]
                    #[should_panic]
                    fn too_long_input_test_block() {
                        super::too_long_input_test_block(
                            &digest::$algorithm_name);
                    }

                    #[test]
                    #[should_panic]
                    fn too_long_input_test_byte() {
                        super::too_long_input_test_byte(
                            &digest::$algorithm_name);
                    }
                }
            }
        }

        fn max_input_test(alg: &'static digest::Algorithm) {
            let mut context = nearly_full_context(alg);
            let next_input = vec![0u8; alg.block_len - 1];
            context.update(&next_input);
            let _ = context.finish(); // no panic
        }

        fn too_long_input_test_block(alg: &'static digest::Algorithm) {
            let mut context = nearly_full_context(alg);
            let next_input = vec![0u8; alg.block_len];
            context.update(&next_input);
            let _ = context.finish(); // should panic
        }

        fn too_long_input_test_byte(alg: &'static digest::Algorithm) {
            let mut context = nearly_full_context(alg);
            let next_input = vec![0u8; alg.block_len - 1];
            context.update(&next_input); // no panic
            context.update(&[0]);
            let _ = context.finish(); // should panic
        }

        fn nearly_full_context(alg: &'static digest::Algorithm)
                               -> digest::Context {
            // All implementations currently support up to 2^64-1 bits
            // of input; according to the spec, SHA-384 and SHA-512
            // support up to 2^128-1, but that's not implemented yet.
            let max_bytes = 1u64 << (64 - 3);
            let max_blocks = max_bytes / (alg.block_len as u64);
            digest::Context {
                algorithm: alg,
                state: alg.initial_state,
                completed_data_blocks: max_blocks - 1,
                pending: [0u8; digest::MAX_BLOCK_LEN],
                num_pending: 0,
            }
        }

        max_input_tests!(SHA256);
        max_input_tests!(SHA512);
    }
}*/
