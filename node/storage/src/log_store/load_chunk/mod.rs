mod bitmap;
mod chunk_data;
mod seal;
mod serde;

use std::cmp::min;

use anyhow::Result;
use ethereum_types::H256;
use ssz_derive::{Decode, Encode};

use crate::log_store::log_manager::data_to_merkle_leaves;
use crate::try_option;
use append_merkle::{Algorithm, MerkleTreeRead, Sha3Algorithm};
use shared_types::{ChunkArray, DataRoot, Merkle};
use tracing::trace;
use zgs_spec::{
    BYTES_PER_LOAD, BYTES_PER_SEAL, BYTES_PER_SECTOR, SEALS_PER_LOAD, SECTORS_PER_LOAD,
    SECTORS_PER_SEAL,
};

use super::SealAnswer;
use chunk_data::EntryBatchData;
use seal::SealInfo;

#[derive(Encode, Decode)]
pub struct EntryBatch {
    seal: SealInfo,
    // the inner data
    data: EntryBatchData,
}

impl EntryBatch {
    pub fn new(load_index_global: u64) -> Self {
        Self {
            seal: SealInfo::new(load_index_global),
            data: EntryBatchData::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl EntryBatch {
    pub fn get_sealed_data(&self, seal_index: u16) -> Option<[u8; BYTES_PER_SEAL]> {
        if self.seal.is_sealed(seal_index) {
            let loaded_slice = self
                .data
                .get(seal_index as usize * BYTES_PER_SEAL, BYTES_PER_SEAL)?;
            Some(loaded_slice.try_into().unwrap())
        } else {
            None
        }
    }

    pub fn get_non_sealed_data(&self, seal_index: u16) -> Option<[u8; BYTES_PER_SEAL]> {
        if !self.seal.is_sealed(seal_index) {
            let loaded_slice = self
                .data
                .get(seal_index as usize * BYTES_PER_SEAL, BYTES_PER_SEAL)?;
            Some(loaded_slice.try_into().unwrap())
        } else {
            None
        }
    }

    /// Get unsealed data
    pub fn get_unsealed_data(&self, start_sector: usize, length_sector: usize) -> Option<Vec<u8>> {
        // If the start position is not aligned and is sealed, we need to load one more word (32 bytes) for unsealing
        let advanced_by_one = if start_sector % SECTORS_PER_SEAL == 0 {
            // If the start position is not aligned, it is no need to load one more word
            false
        } else {
            // otherwise, it depends on if the given offset is seal
            self.seal
                .is_sealed((start_sector / SECTORS_PER_SEAL) as u16)
        };

        let start_byte = start_sector * BYTES_PER_SECTOR;
        let length_byte = length_sector * BYTES_PER_SECTOR;

        // Load data slice with the word for unsealing
        let (mut loaded_data, unseal_mask_seed) = if advanced_by_one {
            let loaded_data_with_hint = self.data.get(start_byte - 32, length_byte + 32)?;

            // TODO (api stable): use `split_array_ref` instead when this api is stable.
            let (unseal_mask_seed, loaded_data) = loaded_data_with_hint.split_at(32);
            let unseal_mask_seed = <[u8; 32]>::try_from(unseal_mask_seed).unwrap();
            (loaded_data.to_vec(), Some(unseal_mask_seed))
        } else {
            (self.data.get(start_byte, length_byte)?.to_vec(), None)
        };

        let incomplete_seal_chunk_length = (BYTES_PER_LOAD - start_byte) % BYTES_PER_SEAL;

        // Unseal the first incomplete sealing chunk (if exists)
        if let Some(unseal_mask_seed) = unseal_mask_seed {
            let data_to_unseal = if loaded_data.len() < incomplete_seal_chunk_length {
                // The loaded data does not cross sealings
                loaded_data.as_mut()
            } else {
                loaded_data[..incomplete_seal_chunk_length].as_mut()
            };

            zgs_seal::unseal_with_mask_seed(data_to_unseal, unseal_mask_seed);
        }

        if loaded_data.len() > incomplete_seal_chunk_length {
            let complete_chunks = &mut loaded_data[incomplete_seal_chunk_length..];
            let start_seal = (start_byte + incomplete_seal_chunk_length) / BYTES_PER_SEAL;

            for (seal_index, data_to_unseal) in complete_chunks
                .chunks_mut(BYTES_PER_SEAL)
                .enumerate()
                .map(|(idx, chunk)| (start_seal + idx, chunk))
            {
                self.seal.unseal(data_to_unseal, seal_index as u16);
            }
        }

        Some(loaded_data)
    }

    /// Return `Error` if the new data overlaps with old data.
    /// Convert `Incomplete` to `Completed` if the chunk is completed after the insertion.
    pub fn insert_data(&mut self, offset: usize, data: Vec<u8>) -> Result<Vec<u16>> {
        self.data.insert_data(offset * BYTES_PER_SECTOR, data)
    }

    pub fn truncate(&mut self, truncated_sector: usize) -> Vec<u16> {
        assert!(truncated_sector > 0 && truncated_sector < SECTORS_PER_LOAD);

        self.data.truncate(truncated_sector * BYTES_PER_SECTOR);
        self.truncate_seal(truncated_sector)
    }

    pub fn into_data_list(self, global_start_entry: u64) -> Vec<ChunkArray> {
        self.data
            .available_range_entries()
            .into_iter()
            .map(|(start_entry, length_entry)| ChunkArray {
                data: self
                    .get_unsealed_data(start_entry, length_entry)
                    .unwrap()
                    .to_vec(),
                start_index: global_start_entry + start_entry as u64,
            })
            .collect()
    }

    fn truncate_seal(&mut self, truncated_sector: usize) -> Vec<u16> {
        let reverted_seal_index = (truncated_sector / SECTORS_PER_SEAL) as u16;

        let first_unseal_index = self.seal.truncated_seal_index(reverted_seal_index);
        let last_unseal_index = ((truncated_sector - 1) / SECTORS_PER_SEAL) as u16;

        let mut to_reseal_set = Vec::with_capacity(SEALS_PER_LOAD);

        for unseal_index in first_unseal_index..=last_unseal_index {
            if !self.seal.is_sealed(unseal_index) {
                continue;
            }

            let truncated_byte = truncated_sector * BYTES_PER_SECTOR;
            let first_unseal_byte = unseal_index as usize * BYTES_PER_SEAL;
            let length = min(truncated_byte - first_unseal_byte, BYTES_PER_SEAL);
            let to_unseal = self
                .data
                .get_mut(first_unseal_byte, length)
                .expect("Sealed chunk should be complete");
            self.seal.unseal(to_unseal, unseal_index);

            to_reseal_set.push(unseal_index)
        }

        // truncate the bitmap
        self.seal.truncate(reverted_seal_index);

        to_reseal_set
    }

    pub fn build_root(&self, is_first_chunk: bool) -> Result<Option<H256>> {
        // Fast check if an incomplete chunk is a full chunk.
        if let EntryBatchData::Incomplete(d) = &self.data {
            if self.get_unsealed_data(SECTORS_PER_LOAD - 1, 1).is_none() {
                if let Some(last_subtree) = d.subtrees.last() {
                    if last_subtree.start_sector + (1 << (last_subtree.subtree_height - 1))
                        != SECTORS_PER_LOAD
                    {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        }
        Ok(Some(
            *try_option!(self.to_merkle_tree(is_first_chunk)?).root(),
        ))
    }

    pub fn submit_seal_result(&mut self, answer: SealAnswer) -> Result<()> {
        let local_seal_index = answer.seal_index as usize % SEALS_PER_LOAD;
        assert!(
            !self.seal.is_sealed(local_seal_index as u16),
            "Duplicated sealing"
        );
        assert_eq!(
            answer.seal_index / SEALS_PER_LOAD as u64,
            self.seal.load_index()
        );

        self.seal.set_seal_context(
            answer.seal_context,
            answer.context_end_seal,
            answer.miner_id,
        );
        let sealing_segment = self
            .data
            .get_mut(local_seal_index * BYTES_PER_SEAL, BYTES_PER_SEAL)
            .expect("Sealing segment should exist");

        sealing_segment.copy_from_slice(&answer.sealed_data);
        self.seal.mark_sealed(local_seal_index as u16);

        Ok(())
    }

    /// This is only called once when the batch is removed from the memory and fully stored in db.
    pub fn set_subtree_list(&mut self, subtree_list: Vec<(usize, usize, DataRoot)>) {
        self.data.set_subtree_list(subtree_list)
    }

    pub fn to_merkle_tree(&self, is_first_chunk: bool) -> Result<Option<Merkle>> {
        let initial_leaves = if is_first_chunk {
            vec![H256::zero()]
        } else {
            vec![]
        };
        let mut merkle = Merkle::new(initial_leaves, 0, None);
        for subtree in self.data.get_subtree_list() {
            trace!(?subtree, "get subtree, leaves={}", merkle.leaves());
            if subtree.start_sector != merkle.leaves() {
                let leaf_data = try_option!(
                    self.get_unsealed_data(merkle.leaves(), subtree.start_sector - merkle.leaves())
                );
                merkle.append_list(data_to_merkle_leaves(&leaf_data).expect("aligned"));
            }
            merkle.append_subtree(subtree.subtree_height, subtree.root)?;
        }
        if merkle.leaves() != SECTORS_PER_LOAD {
            let leaf_data = try_option!(
                self.get_unsealed_data(merkle.leaves(), SECTORS_PER_LOAD - merkle.leaves())
            );
            merkle.append_list(data_to_merkle_leaves(&leaf_data).expect("aligned"));
        }
        // TODO(zz): Optimize.
        for index in 0..merkle.leaves() {
            if merkle.leaf_at(index)?.is_none() {
                if let Some(leaf_data) = self.get_unsealed_data(index, 1) {
                    merkle.fill_leaf(index, Sha3Algorithm::leaf(&leaf_data));
                }
            }
        }
        Ok(Some(merkle))
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryBatch, SealAnswer};
    use ethereum_types::H256;
    use zgs_spec::{
        BYTES_PER_SEAL, BYTES_PER_SECTOR, SEALS_PER_LOAD, SECTORS_PER_LOAD, SECTORS_PER_SEAL,
    };
    const LOAD_INDEX: u64 = 1;
    fn seal(
        batch: &mut EntryBatch,
        seal_index: u16,
        context_digest: H256,
        context_end_seal_local: u64,
    ) {
        let miner_id = H256([33u8; 32]);
        let mut data = batch.get_non_sealed_data(seal_index).unwrap();
        zgs_seal::seal(
            &mut data,
            &miner_id,
            &context_digest,
            LOAD_INDEX * SECTORS_PER_LOAD as u64 + seal_index as u64 * SECTORS_PER_SEAL as u64,
        );
        batch
            .submit_seal_result(SealAnswer {
                seal_index: LOAD_INDEX * SEALS_PER_LOAD as u64 + seal_index as u64,
                version: 0,
                sealed_data: data,
                miner_id,
                seal_context: context_digest,
                context_end_seal: LOAD_INDEX * SEALS_PER_LOAD as u64 + context_end_seal_local,
            })
            .unwrap();
    }

    #[test]
    fn test_seal_single() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL]).unwrap();

        const DIGEST: H256 = H256([22u8; 32]);
        seal(&mut batch, 0, DIGEST, 1);

        assert_eq!(
            batch.get_unsealed_data(0, SECTORS_PER_SEAL).unwrap(),
            vec![11; SECTORS_PER_SEAL * BYTES_PER_SECTOR]
        );
        assert_eq!(
            batch.get_unsealed_data(1, SECTORS_PER_SEAL - 1).unwrap(),
            vec![11; (SECTORS_PER_SEAL - 1) * BYTES_PER_SECTOR]
        );
    }

    fn check_two_seals(batch: &EntryBatch) {
        assert_eq!(
            batch.get_unsealed_data(0, SECTORS_PER_SEAL).unwrap(),
            vec![11; SECTORS_PER_SEAL * BYTES_PER_SECTOR]
        );
        assert_eq!(
            batch
                .get_unsealed_data(SECTORS_PER_SEAL, SECTORS_PER_SEAL)
                .unwrap(),
            vec![11; SECTORS_PER_SEAL * BYTES_PER_SECTOR]
        );
        assert_eq!(
            batch.get_unsealed_data(1, SECTORS_PER_SEAL - 1).unwrap(),
            vec![11; (SECTORS_PER_SEAL - 1) * BYTES_PER_SECTOR]
        );
        assert_eq!(
            batch.get_unsealed_data(1, SECTORS_PER_SEAL).unwrap(),
            vec![11; SECTORS_PER_SEAL * BYTES_PER_SECTOR]
        );
        assert_eq!(
            batch
                .get_unsealed_data(1, 2 * SECTORS_PER_SEAL - 1)
                .unwrap(),
            vec![11; (2 * SECTORS_PER_SEAL - 1) * BYTES_PER_SECTOR]
        );
    }

    #[test]
    fn test_seal_mono_context() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        const DIGEST: H256 = H256([22u8; 32]);
        seal(&mut batch, 0, DIGEST, 2);
        seal(&mut batch, 1, DIGEST, 2);

        check_two_seals(&batch);
    }

    #[test]
    fn test_seal_mono_context_reorder() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        const DIGEST: H256 = H256([22u8; 32]);
        seal(&mut batch, 1, DIGEST, 2);
        seal(&mut batch, 0, DIGEST, 2);

        check_two_seals(&batch);
    }

    #[test]
    fn test_seal_mono_context_partial() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        const DIGEST: H256 = H256([22u8; 32]);
        seal(&mut batch, 1, DIGEST, 2);

        check_two_seals(&batch);
    }

    #[test]
    fn test_seal_hete_context() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        const DIGEST0: H256 = H256([22u8; 32]);
        const DIGEST1: H256 = H256([33u8; 32]);

        seal(&mut batch, 0, DIGEST0, 1);
        seal(&mut batch, 1, DIGEST1, 2);

        check_two_seals(&batch);
    }

    #[test]
    fn test_seal_hete_context_reord() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        const DIGEST0: H256 = H256([22u8; 32]);
        const DIGEST1: H256 = H256([33u8; 32]);

        seal(&mut batch, 1, DIGEST1, 2);
        seal(&mut batch, 0, DIGEST0, 1);

        check_two_seals(&batch);
    }

    #[test]
    fn test_seal_hete_context_partial() {
        let mut batch = EntryBatch::new(LOAD_INDEX);
        batch.insert_data(0, vec![11; BYTES_PER_SEAL * 2]).unwrap();

        // const DIGEST0: H256 = H256([22u8; 32]);
        const DIGEST1: H256 = H256([33u8; 32]);

        seal(&mut batch, 1, DIGEST1, 2);

        check_two_seals(&batch);
    }
}
