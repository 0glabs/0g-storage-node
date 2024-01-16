use ethereum_types::H256;
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use static_assertions::const_assert;

use zgs_seal;
use zgs_spec::{SEALS_PER_LOAD, SECTORS_PER_LOAD, SECTORS_PER_SEAL};

use super::bitmap::WrappedBitmap;

#[derive(DeriveEncode, DeriveDecode)]
pub struct SealContextInfo {
    /// The context digest for this seal group
    context_digest: H256,
    /// The end position (exclusive) indexed by sectors
    end_seal_index: u16,
}

type ChunkSealBitmap = WrappedBitmap<SEALS_PER_LOAD>;
const_assert!(SEALS_PER_LOAD <= u128::BITS as usize);

#[derive(Default, DeriveEncode, DeriveDecode)]
pub struct SealInfo {
    // a bitmap specify which sealing chunks have been sealed
    bitmap: ChunkSealBitmap,
    // the batch_offset (seal chunks) of the EntryBatch this seal info belongs to
    load_index: u64,
    // the miner Id for sealing this chunk, zero representing doesn't exists
    miner_id: H256,
    // seal context information, indexed by u16. Get a position has never been set is undefined behaviour.
    seal_contexts: Vec<SealContextInfo>,
}

// Basic interfaces
impl SealInfo {
    pub fn new(load_index: u64) -> Self {
        Self {
            load_index,
            ..Default::default()
        }
    }

    pub fn is_sealed(&self, seal_index: u16) -> bool {
        self.bitmap.get(seal_index as usize)
    }

    pub fn mark_sealed(&mut self, seal_index: u16) {
        self.bitmap.set(seal_index as usize, true);
    }

    pub fn load_index(&self) -> u64 {
        self.load_index
    }

    pub fn global_seal_sector(&self, index: u16) -> u64 {
        (self.load_index as usize * SECTORS_PER_LOAD + index as usize * SECTORS_PER_SEAL) as u64
    }
}

// Interfaces for maintaining context info
impl SealInfo {
    fn context_index(&self, seal_index: u16) -> usize {
        match self
            .seal_contexts
            .binary_search_by_key(&(seal_index + 1), |x| x.end_seal_index)
        {
            Ok(x) | Err(x) => x,
        }
    }

    pub fn get_seal_context_digest(&self, seal_index: u16) -> Option<H256> {
        self.seal_contexts
            .get(self.context_index(seal_index))
            .map(|x| x.context_digest)
    }

    pub fn set_seal_context(
        &mut self,
        context_digest: H256,
        global_end_seal_index: u64,
        miner_id: H256,
    ) {
        // 1. Check consistency of the miner id.
        if self.miner_id.is_zero() {
            self.miner_id = miner_id;
        } else {
            assert!(
                self.miner_id == miner_id,
                "miner_id setting is inconsistent with db"
            );
        }

        // 2. Compute the local end_seal_index
        let end_seal_index = global_end_seal_index - self.load_index * SEALS_PER_LOAD as u64;
        let end_seal_index = std::cmp::min(end_seal_index as u16, SEALS_PER_LOAD as u16);
        let new_context = SealContextInfo {
            context_digest,
            end_seal_index,
        };

        // 3. Update the seal context array by cases
        let insert_position = self.context_index(end_seal_index - 1);

        if let Some(existing_context) = self.seal_contexts.get(insert_position) {
            if existing_context.context_digest == new_context.context_digest {
                // Case 1: the new context is consistent with existing contexts (nothing to do)
            } else {
                // Case 2: the new context should be inserted in the middle (may not happen)
                self.seal_contexts.insert(insert_position, new_context);
            }
        } else {
            // Case 3: the new context exceeds the upper bound of existing contexts
            self.seal_contexts.push(new_context);
        }
    }
}

impl SealInfo {
    pub fn truncate(&mut self, reverted_seal_index: u16) {
        // TODO (kevin): have issue in some cases
        let truncated_context_index = self.context_index(reverted_seal_index);
        let truncated_seal_index = self.truncated_seal_index(reverted_seal_index);

        self.bitmap.truncate(truncated_seal_index);
        self.seal_contexts.truncate(truncated_context_index);
    }

    pub fn truncated_seal_index(&self, reverted_seal_index: u16) -> u16 {
        let truncated_context = self.context_index(reverted_seal_index);
        if truncated_context == 0 {
            0
        } else {
            self.seal_contexts
                .get(truncated_context - 1)
                .unwrap()
                .end_seal_index
        }
    }
}

impl SealInfo {
    pub fn unseal(&self, data: &mut [u8], index: u16) {
        if !self.is_sealed(index) {
            return;
        }
        let seal_context = self
            .get_seal_context_digest(index)
            .expect("cannot unseal non-sealed data");
        zgs_seal::unseal(
            data,
            &self.miner_id,
            &seal_context,
            self.global_seal_sector(index),
        );
    }

    #[cfg(test)]
    pub fn seal(&self, data: &mut [u8], index: u16) {
        if self.is_sealed(index) {
            return;
        }
        let seal_context = self
            .get_seal_context_digest(index)
            .expect("cannot unseal non-sealed data");
        zgs_seal::seal(
            data,
            &self.miner_id,
            &seal_context,
            self.global_seal_sector(index),
        );
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;
    use hex_literal::hex;
    use rand::{rngs::StdRng, RngCore, SeedableRng};
    use zgs_seal;
    use zgs_spec::BYTES_PER_SEAL;

    use super::{SealContextInfo, SealInfo};

    const TEST_MINER_ID: H256 = H256(hex!(
        "003d82782c78262bada18a22f5f982d2b43934d5541e236ca3781ddc8c911cb8"
    ));

    #[test]
    fn get_seal_context() {
        let mut random = StdRng::seed_from_u64(149);

        let mut context1 = H256::default();
        let mut context2 = H256::default();
        let mut context3 = H256::default();
        random.fill_bytes(&mut context1.0);
        random.fill_bytes(&mut context2.0);
        random.fill_bytes(&mut context3.0);

        let mut sealer = SealInfo::new(0);
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context1,
            end_seal_index: 2,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context2,
            end_seal_index: 3,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context3,
            end_seal_index: 6,
        });

        assert_eq!(sealer.get_seal_context_digest(0), Some(context1));
        assert_eq!(sealer.get_seal_context_digest(1), Some(context1));
        assert_eq!(sealer.get_seal_context_digest(2), Some(context2));
        assert_eq!(sealer.get_seal_context_digest(3), Some(context3));
        assert_eq!(sealer.get_seal_context_digest(4), Some(context3));
        assert_eq!(sealer.get_seal_context_digest(5), Some(context3));
        assert_eq!(sealer.get_seal_context_digest(6), None);

        assert_eq!(sealer.truncated_seal_index(0), 0);
        assert_eq!(sealer.truncated_seal_index(1), 0);
        assert_eq!(sealer.truncated_seal_index(2), 2);
        assert_eq!(sealer.truncated_seal_index(3), 3);
        assert_eq!(sealer.truncated_seal_index(4), 3);
        assert_eq!(sealer.truncated_seal_index(5), 3);
        assert_eq!(sealer.truncated_seal_index(6), 6);
    }

    #[test]
    fn unseal_chunks() {
        let mut random = StdRng::seed_from_u64(137);
        let mut unsealed_data = vec![0u8; BYTES_PER_SEAL * 10];
        random.fill_bytes(&mut unsealed_data);
        let mut data = unsealed_data.clone();

        let mut context1 = H256::default();
        let mut context2 = H256::default();
        let mut context3 = H256::default();
        random.fill_bytes(&mut context1.0);
        random.fill_bytes(&mut context2.0);
        random.fill_bytes(&mut context3.0);

        let mut sealer = SealInfo::new(100);
        sealer.miner_id = TEST_MINER_ID;

        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context1,
            end_seal_index: 2,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context2,
            end_seal_index: 5,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context3,
            end_seal_index: 10,
        });

        // skip seal 6, 3, 9
        for idx in [1, 7, 2, 5, 0, 8, 4].into_iter() {
            sealer.seal(
                &mut data[idx * BYTES_PER_SEAL..(idx + 1) * BYTES_PER_SEAL],
                idx as u16,
            );
            sealer.bitmap.set(idx, true);
        }

        let partial_hint = &data[BYTES_PER_SEAL * 5 + 64..BYTES_PER_SEAL * 5 + 96];
        let mut tmp_data = data.clone();
        zgs_seal::unseal_with_mask_seed(
            &mut tmp_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6],
            partial_hint,
        );
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6],
            &unsealed_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(&mut tmp_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6], 5);
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6],
            &unsealed_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(&mut tmp_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7], 6);
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7],
            &unsealed_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(
            &mut tmp_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96],
            7,
        );
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96],
            &unsealed_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96]
        );
    }
}
