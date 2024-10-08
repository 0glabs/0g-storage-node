use ethereum_types::U256;
use tiny_keccak::{Hasher, Keccak};
use zgs_spec::{SECTORS_PER_LOAD, SECTORS_PER_MAX_MINING_RANGE};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct RecallRange {
    pub start_position: u64,
    pub mining_length: u64,
    pub shard_mask: u64,
    pub shard_id: u64,
}

impl RecallRange {
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Keccak::v256();
        hasher.update(&[0u8; 24]);
        hasher.update(&self.start_position.to_be_bytes());

        hasher.update(&[0u8; 24]);
        hasher.update(&self.mining_length.to_be_bytes());

        hasher.update(&[0u8; 24]);
        hasher.update(&self.shard_id.to_be_bytes());

        hasher.update(&[0u8; 24]);
        hasher.update(&self.shard_mask.to_be_bytes());

        let mut output = [0u8; 32];
        hasher.finalize(&mut output);
        output
    }

    pub fn load_position(&self, seed: [u8; 32]) -> Option<u64> {
        let origin_recall_offset = U256::from_big_endian(&seed)
            .checked_rem(U256::from((self.mining_length as usize) / SECTORS_PER_LOAD))?;
        let origin_recall_offset = origin_recall_offset.as_u64();
        let recall_offset = (origin_recall_offset & self.shard_mask) | self.shard_id;

        self.start_position
            .checked_add(recall_offset * SECTORS_PER_LOAD as u64)
    }

    pub fn difficulty_scale_x64(&self, flow_length: u64) -> U256 {
        let no_shard_mine_length = std::cmp::min(flow_length, SECTORS_PER_MAX_MINING_RANGE as u64);
        let sharded_mine_length = std::cmp::min(
            flow_length >> self.shard_mask.count_zeros(),
            SECTORS_PER_MAX_MINING_RANGE as u64,
        );

        (U256::from(no_shard_mine_length) << 64) / sharded_mine_length
    }
}

impl From<RecallRange> for contract_interface::RecallRange {
    fn from(value: RecallRange) -> Self {
        Self {
            start_position: value.start_position.into(),
            mine_length: value.mining_length.into(),
            shard_mask: value.shard_mask,
            shard_id: value.shard_id,
        }
    }
}
