use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};
use std::{cell::RefCell, path::PathBuf, rc::Rc};

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

impl TryFrom<&str> for ShardConfig {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.trim().split('/').map(|s| s.trim()).collect();

        if parts.len() != 2 {
            return Err("Incorrect format, expected like: '0 / 8'".into());
        }

        let numerator = parts[0]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;
        let denominator = parts[1]
            .parse::<usize>()
            .map_err(|e| format!("Cannot parse shard position {:?}", e))?;

        Self::new(numerator, denominator)
    }
}

impl TryFrom<&Option<String>> for ShardConfig {
    type Error = String;

    fn try_from(value: &Option<String>) -> Result<Self, Self::Error> {
        if let Some(position) = value {
            <ShardConfig as TryFrom<&str>>::try_from(position)
        } else {
            Ok(Self::default())
        }
    }
}

impl ShardConfig {
    pub fn new(id: usize, num: usize) -> Result<Self, String> {
        let config = ShardConfig {
            shard_id: id,
            num_shard: num,
        };

        config.validate()?;

        Ok(config)
    }

    pub fn miner_shard_mask(&self) -> u64 {
        !(self.num_shard - 1) as u64
    }

    pub fn miner_shard_id(&self) -> u64 {
        self.shard_id as u64
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.shard_id >= self.num_shard {
            return Err(format!(
                "Incorrect shard_id: expected [0, {}), actual {}",
                self.num_shard, self.shard_id
            ));
        }

        if self.num_shard == 0 {
            return Err("Shard num is 0".into());
        }

        if !self.num_shard.is_power_of_two() {
            return Err(format!(
                "Incorrect shard group bytes: {}, should be power of two",
                self.num_shard
            ));
        }

        Ok(())
    }

    pub fn in_range(&self, segment_index: u64) -> bool {
        segment_index as usize % self.num_shard == self.shard_id
    }

    pub fn next_segment_index(&self, current: usize, start_index: usize) -> usize {
        // `shift` should be 0 if `current` was returned by the same config.
        let shift = (start_index + current + self.num_shard - self.shard_id) % self.num_shard;
        current + self.num_shard - shift
    }

    /// Whether `self` intersect with the `other` shard config.
    pub fn intersect(&self, other: &ShardConfig) -> bool {
        let ShardConfig {
            num_shard: mut left_num_shard,
            shard_id: mut left_shard_id,
        } = self;
        let ShardConfig {
            num_shard: mut right_num_shard,
            shard_id: mut right_shard_id,
        } = other;

        while left_num_shard != right_num_shard {
            if left_num_shard < right_num_shard {
                right_num_shard /= 2;
                right_shard_id /= 2;
            } else {
                left_num_shard /= 2;
                left_shard_id /= 2;
            }
        }

        left_shard_id == right_shard_id
    }
}

struct ShardSegmentTreeNode {
    pub num_shard: usize,
    pub covered: bool,
    pub childs: [Option<Rc<RefCell<ShardSegmentTreeNode>>>; 2],
}

impl ShardSegmentTreeNode {
    pub fn new(num_shard: usize) -> Self {
        ShardSegmentTreeNode {
            num_shard,
            covered: false,
            childs: [None, None],
        }
    }

    fn push_down(&mut self) {
        if self.childs[0].is_none() {
            for i in 0..2 {
                self.childs[i] = Some(Rc::new(RefCell::new(ShardSegmentTreeNode::new(
                    self.num_shard << 1,
                ))));
            }
        }
    }

    fn update(&mut self) {
        let mut covered = true;
        for i in 0..2 {
            if let Some(child) = &self.childs[i] {
                covered = covered && child.borrow().covered;
            }
        }
        self.covered = covered;
    }

    pub fn insert(&mut self, num_shard: usize, shard_id: usize) {
        if self.covered {
            return;
        }
        if num_shard == self.num_shard {
            self.covered = true;
            return;
        }
        self.push_down();
        if let Some(child) = &self.childs[shard_id % 2] {
            child.borrow_mut().insert(num_shard, shard_id >> 1);
        }
        self.update();
    }
}

pub fn all_shards_available(shard_configs: Vec<ShardConfig>) -> bool {
    let mut root = ShardSegmentTreeNode::new(1);
    for shard_config in shard_configs.iter() {
        if shard_config.validate().is_err() {
            continue;
        }
        root.insert(shard_config.num_shard, shard_config.shard_id);
        if root.covered {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::config::all_shards_available;

    use super::ShardConfig;

    fn new_config(id: usize, num: usize) -> ShardConfig {
        ShardConfig::new(id, num).unwrap()
    }

    #[test]
    fn test_all_shards_available() {
        assert!(all_shards_available(vec![
            ShardConfig {
                shard_id: 3,
                num_shard: 8
            },
            ShardConfig {
                shard_id: 7,
                num_shard: 8
            },
            ShardConfig {
                shard_id: 0,
                num_shard: 4
            },
            ShardConfig {
                shard_id: 1,
                num_shard: 4
            },
            ShardConfig {
                shard_id: 0,
                num_shard: 2
            },
            ShardConfig {
                shard_id: 0,
                num_shard: 1 << 25
            },
        ]));
        assert!(!all_shards_available(vec![
            ShardConfig {
                shard_id: 0,
                num_shard: 4
            },
            ShardConfig {
                shard_id: 1,
                num_shard: 4
            },
            ShardConfig {
                shard_id: 3,
                num_shard: 8
            },
            ShardConfig {
                shard_id: 0,
                num_shard: 2
            },
        ]));
    }

    #[test]
    fn test_shard_intersect() {
        // 1 shard
        assert_eq!(new_config(0, 1).intersect(&new_config(0, 1)), true);

        // either is 1 shard
        assert_eq!(new_config(0, 1).intersect(&new_config(0, 2)), true);
        assert_eq!(new_config(0, 1).intersect(&new_config(1, 2)), true);
        assert_eq!(new_config(0, 2).intersect(&new_config(0, 1)), true);
        assert_eq!(new_config(1, 2).intersect(&new_config(0, 1)), true);

        // same shards
        assert_eq!(new_config(1, 4).intersect(&new_config(0, 4)), false);
        assert_eq!(new_config(1, 4).intersect(&new_config(1, 4)), true);
        assert_eq!(new_config(1, 4).intersect(&new_config(2, 4)), false);
        assert_eq!(new_config(1, 4).intersect(&new_config(3, 4)), false);

        // left shards is less
        assert_eq!(new_config(1, 2).intersect(&new_config(0, 4)), false);
        assert_eq!(new_config(1, 2).intersect(&new_config(1, 4)), false);
        assert_eq!(new_config(1, 2).intersect(&new_config(2, 4)), true);
        assert_eq!(new_config(1, 2).intersect(&new_config(3, 4)), true);

        // right shards is less
        assert_eq!(new_config(1, 4).intersect(&new_config(0, 2)), true);
        assert_eq!(new_config(1, 4).intersect(&new_config(1, 2)), false);
        assert_eq!(new_config(2, 4).intersect(&new_config(0, 2)), false);
        assert_eq!(new_config(2, 4).intersect(&new_config(1, 2)), true);
    }
}
