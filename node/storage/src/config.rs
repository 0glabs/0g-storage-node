use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use std::path::PathBuf;

pub const SHARD_CONFIG_KEY: &str = "shard_config";

#[derive(Clone)]
pub struct Config {
    pub db_dir: PathBuf,
}

#[derive(Clone, Copy, Debug, Decode, Encode, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShardConfig {
    pub shard_id: usize,
    pub num_shard: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_id: 0,
            num_shard: 1,
        }
    }
}

impl ShardConfig {
    pub fn new(shard_position: &Option<String>) -> Result<Self, String> {
        let (id, num) = if let Some(position) = shard_position {
            Self::parse_position(position)?
        } else {
            (0, 1)
        };

        if id >= num {
            return Err(format!(
                "Incorrect shard_id: expected [0, {}), actual {}",
                num, id
            ));
        }

        if !num.is_power_of_two() {
            return Err(format!(
                "Incorrect shard group bytes: {}, should be power of two",
                num
            ));
        }
        Ok(ShardConfig {
            shard_id: id,
            num_shard: num,
        })
    }

    pub fn miner_shard_mask(&self) -> u64 {
        !(self.num_shard - 1) as u64
    }

    pub fn miner_shard_id(&self) -> u64 {
        self.shard_id as u64
    }

    pub fn in_range(&self, segment_index: u64) -> bool {
        segment_index as usize % self.num_shard == self.shard_id
    }

    pub fn parse_position(input: &str) -> Result<(usize, usize), String> {
        let parts: Vec<&str> = input.trim().split('/').map(|s| s.trim()).collect();

        if parts.len() != 2 {
            return Err("Incorrect format, expected like: '0 / 8'".into());
        }

        let numerator = parts[0]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;
        let denominator = parts[1]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;

        Ok((numerator, denominator))
    }

    pub fn next_segment_index(&self, current: usize, start_index: usize) -> usize {
        // `shift` should be 0 if `current` was returned by the same config.
        let shift = (start_index + current + self.num_shard - self.shard_id) % self.num_shard;
        current + self.num_shard - shift
    }
}
