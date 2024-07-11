use super::metrics::*;
use crate::recall_range::RecallRange;
use crate::{MineRangeConfig, PoraLoader};
use blake2::{Blake2b512, Digest};
use contract_interface::zgs_flow::MineContext;
use ethereum_types::{H256, U256};
use lighthouse_metrics::inc_counter;
use storage::log_store::MineLoadChunk;
use tiny_keccak::{Hasher, Keccak};
use zgs_spec::{BYTES_PER_SCRATCHPAD, BYTES_PER_SEAL, SECTORS_PER_LOAD, SECTORS_PER_SEAL};

pub const BLAKE2B_OUTPUT_BYTES: usize = 64;
pub const KECCAK256_OUTPUT_BYTES: usize = 32;

fn keccak(input: impl AsRef<[u8]>) -> [u8; KECCAK256_OUTPUT_BYTES] {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(input.as_ref());
    hasher.finalize(&mut output);
    output
}

pub(crate) struct Miner<'a> {
    pub range: RecallRange,
    pub miner_id: &'a H256,
    pub context: &'a MineContext,
    pub target_quality: &'a U256,
    pub loader: &'a dyn PoraLoader,
    pub mine_range_config: &'a MineRangeConfig,
}
#[derive(Debug)]
pub struct AnswerWithoutProof {
    pub context_digest: H256,
    pub context_flow_root: H256,
    pub nonce: H256,
    pub miner_id: H256,
    pub range: RecallRange,
    pub recall_position: u64,
    pub seal_offset: usize,
    pub sealed_data: [u8; BYTES_PER_SEAL],
}

impl<'a> Miner<'a> {
    pub async fn batch_iteration(
        &self,
        nonce: H256,
        batch_size: usize,
    ) -> Option<AnswerWithoutProof> {
        for i in 0..batch_size {
            let bytes = i.to_ne_bytes();
            let mut current_nonce = nonce;
            for (pos, b) in bytes.into_iter().enumerate() {
                current_nonce.0[pos] ^= b;
            }
            if let Some(answer) = self.iteration(current_nonce).await {
                return Some(answer);
            }
        }
        None
    }

    pub async fn iteration(&self, nonce: H256) -> Option<AnswerWithoutProof> {
        inc_counter(&SCRATCH_PAD_ITER_COUNT);
        let ScratchPad {
            scratch_pad,
            recall_seed,
            pad_seed,
        } = self.make_scratch_pad(&nonce);

        let recall_position = self.range.load_position(recall_seed)?;
        if !self.mine_range_config.is_covered(recall_position).unwrap() {
            trace!(
                "recall offset not in range: recall_offset={}",
                recall_position,
            );
            return None;
        }

        inc_counter(&LOADING_COUNT);
        let MineLoadChunk {
            loaded_chunk,
            avalibilities,
        } = self
            .loader
            .load_sealed_data(recall_position / SECTORS_PER_LOAD as u64)
            .await?;

        let scratch_pad: [[u8; BYTES_PER_SEAL]; BYTES_PER_SCRATCHPAD / BYTES_PER_SEAL] =
            unsafe { std::mem::transmute(scratch_pad) };

        for ((idx, mut sealed_data), scratch_pad) in loaded_chunk
            .into_iter()
            .enumerate()
            .zip(scratch_pad.iter().cycle())
            .zip(avalibilities.into_iter())
            .filter_map(|(data, avaliable)| avaliable.then_some(data))
        {
            inc_counter(&PAD_MIX_COUNT);
            // Rust can optimize this loop well.
            for (x, y) in sealed_data.iter_mut().zip(scratch_pad.iter()) {
                *x ^= y;
            }

            let quality = self.pora(idx, &sealed_data, pad_seed);
            let quality_scale = self.range.shard_mask.count_zeros();
            if quality <= U256::MAX >> quality_scale
                && quality << quality_scale <= *self.target_quality
            {
                debug!(
                    "Find a PoRA valid answer, quality: {}, target_quality {}, scale {}",
                    U256::MAX / quality,
                    U256::MAX / self.target_quality,
                    quality_scale
                );
                inc_counter(&HIT_COUNT);
                // Undo mix data when find a valid solition
                for (x, y) in sealed_data.iter_mut().zip(scratch_pad.iter()) {
                    *x ^= y;
                }
                return Some(AnswerWithoutProof {
                    context_digest: H256::from(self.context.digest),
                    context_flow_root: self.context.flow_root.into(),
                    nonce,
                    miner_id: *self.miner_id,
                    range: self.range,
                    recall_position: recall_position + idx as u64 * SECTORS_PER_SEAL as u64,
                    seal_offset: idx,
                    sealed_data,
                });
            }
        }
        None
    }

    fn make_scratch_pad(&self, nonce: &H256) -> ScratchPad {
        let mut digest: [u8; BLAKE2B_OUTPUT_BYTES] = {
            let mut hasher = Blake2b512::new();
            hasher.update(self.miner_id);
            hasher.update(nonce);
            hasher.update(self.context.digest);
            hasher.update(self.range.digest());
            hasher.finalize().into()
        };

        let pad_seed = digest;

        let mut scratch_pad =
            [[0u8; BLAKE2B_OUTPUT_BYTES]; BYTES_PER_SCRATCHPAD / BLAKE2B_OUTPUT_BYTES];
        for scratch_pad_cell in scratch_pad.iter_mut() {
            digest = Blake2b512::new().chain_update(digest).finalize().into();
            *scratch_pad_cell = digest;
        }

        let scratch_pad: [u8; BYTES_PER_SCRATCHPAD] = unsafe { std::mem::transmute(scratch_pad) };
        let recall_seed: [u8; KECCAK256_OUTPUT_BYTES] = keccak(digest);

        ScratchPad {
            scratch_pad,
            recall_seed,
            pad_seed,
        }
    }

    #[inline]
    fn pora(
        &self,
        seal_index: usize,
        mixed_data: &[u8; BYTES_PER_SEAL],
        pad_seed: [u8; BLAKE2B_OUTPUT_BYTES],
    ) -> U256 {
        let mut hasher = Blake2b512::new();
        hasher.update([0u8; 24]);
        hasher.update(seal_index.to_be_bytes());

        hasher.update(pad_seed);
        hasher.update([0u8; 32]);

        hasher.update(mixed_data);

        let digest = hasher.finalize();

        U256::from_big_endian(&digest[0..32])
    }
}

struct ScratchPad {
    scratch_pad: [u8; BYTES_PER_SCRATCHPAD],
    recall_seed: [u8; KECCAK256_OUTPUT_BYTES],
    pad_seed: [u8; BLAKE2B_OUTPUT_BYTES],
}
