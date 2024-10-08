mod file_location_cache;
pub mod test_util;

use serde::Deserialize;

pub use crate::file_location_cache::FileLocationCache;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub max_entries_total: usize,
    pub max_entries_per_file: usize,
    pub entry_expiration_time_secs: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_entries_total: 1000000,
            max_entries_per_file: 4,
            entry_expiration_time_secs: 86400,
        }
    }
}
