use crate::{CustomMineRange, PoraLoader};
use blake2::{Blake2b512, Digest};
use contract_interface::zgs_flow::MineContext;
use ethereum_types::{H256, U256};
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
    pub start_position: u64,
    pub mining_length: u64,
    pub miner_id: &'a H256,
    pub context: &'a MineContext,
    pub target_quality: &'a U256,
    pub loader: &'a dyn PoraLoader,
    pub custom_mine_range: &'a CustomMineRange,
}
#[derive(Debug)]
pub struct AnswerWithoutProof {
    pub context_digest: H256,
    pub context_flow_root: H256,
    pub nonce: H256,
    pub miner_id: H256,
    pub start_position: u64,
    pub mining_length: u64,
    pub recall_position: u64,
    pub seal_offset: usize,
    pub sealed_data: [u8; BYTES_PER_SEAL],
}

impl<'a> Miner<'a> {
    pub async fn iteration(&self, nonce: H256) -> Option<AnswerWithoutProof> {
        let (scratch_pad, recall_seed) = self.make_scratch_pad(&nonce);

        if self.mining_length == 0 {
            return None;
        }

        let (_, recall_offset) = U256::from_big_endian(&recall_seed)
            .div_mod(U256::from((self.mining_length as usize) / SECTORS_PER_LOAD));
        let recall_offset = recall_offset.as_u64();
        if !self
            .custom_mine_range
            .is_covered(self.start_position + recall_offset * SECTORS_PER_LOAD as u64)
            .unwrap()
        {
            trace!(
                "recall offset not in range: recall_offset={}, range={:?}",
                recall_offset,
                self.custom_mine_range
            );
            return None;
        }

        let MineLoadChunk {
            loaded_chunk,
            avalibilities,
        } = self
            .loader
            .load_sealed_data(self.start_position / SECTORS_PER_LOAD as u64 + recall_offset)
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
            // Rust can optimize this loop well.
            for (x, y) in sealed_data.iter_mut().zip(scratch_pad.iter()) {
                *x ^= y;
            }

            let quality = self.pora(idx, &nonce, &sealed_data);
            if &quality <= self.target_quality {
                debug!("Find a PoRA valid answer, quality: {}", quality);
                // Undo mix data when find a valid solition
                for (x, y) in sealed_data.iter_mut().zip(scratch_pad.iter()) {
                    *x ^= y;
                }
                return Some(AnswerWithoutProof {
                    context_digest: H256::from(self.context.digest),
                    context_flow_root: self.context.flow_root.into(),
                    nonce,
                    miner_id: *self.miner_id,
                    start_position: self.start_position,
                    mining_length: self.mining_length,
                    recall_position: self.start_position
                        + recall_offset * SECTORS_PER_LOAD as u64
                        + idx as u64 * SECTORS_PER_SEAL as u64,
                    seal_offset: idx,
                    sealed_data,
                });
            }
        }
        None
    }

    fn make_scratch_pad(
        &self,
        nonce: &H256,
    ) -> ([u8; BYTES_PER_SCRATCHPAD], [u8; KECCAK256_OUTPUT_BYTES]) {
        let mut digest: [u8; BLAKE2B_OUTPUT_BYTES] = {
            let mut hasher = Blake2b512::new();
            hasher.update(self.miner_id);
            hasher.update(nonce);
            hasher.update(self.context.digest);

            hasher.update([0u8; 24]);
            hasher.update(self.start_position.to_be_bytes());

            hasher.update([0u8; 24]);
            hasher.update(self.mining_length.to_be_bytes());

            hasher.finalize().into()
        };

        let mut scratch_pad =
            [[0u8; BLAKE2B_OUTPUT_BYTES]; BYTES_PER_SCRATCHPAD / BLAKE2B_OUTPUT_BYTES];
        for scratch_pad_cell in scratch_pad.iter_mut() {
            digest = Blake2b512::new().chain_update(digest).finalize().into();
            *scratch_pad_cell = digest;
        }

        let scratch_pad: [u8; BYTES_PER_SCRATCHPAD] = unsafe { std::mem::transmute(scratch_pad) };
        let recall_seed: [u8; KECCAK256_OUTPUT_BYTES] = keccak(digest);

        (scratch_pad, recall_seed)
    }

    #[inline]
    fn pora(&self, seal_index: usize, nonce: &H256, mixed_data: &[u8; BYTES_PER_SEAL]) -> U256 {
        let mut hasher = Blake2b512::new();
        hasher.update([0u8; 24]);
        hasher.update(seal_index.to_be_bytes());

        hasher.update(self.miner_id);
        hasher.update(nonce);
        hasher.update(self.context.digest);

        hasher.update([0u8; 24]);
        hasher.update(self.start_position.to_be_bytes());

        hasher.update([0u8; 24]);
        hasher.update(self.mining_length.to_be_bytes());

        hasher.update([0u8; 64]);
        hasher.update(mixed_data);

        let digest = hasher.finalize();

        U256::from_big_endian(&digest[0..32])
    }
}
