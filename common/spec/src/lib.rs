pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;
pub const TB: usize = 1024 * GB;

pub const BYTES_PER_SECTOR: usize = 256;
pub const BYTES_PER_SEAL: usize = 4 * KB;
pub const BYTES_PER_SCRATCHPAD: usize = 64 * KB;
pub const BYTES_PER_LOAD: usize = 256 * KB;
pub const BYTES_PER_PRICING: usize = 8 * GB;
pub const BYTES_PER_MAX_MINING_RANGE: usize = 8 * TB;

pub const SECTORS_PER_LOAD: usize = BYTES_PER_LOAD / BYTES_PER_SECTOR;
pub const SECTORS_PER_SEAL: usize = BYTES_PER_SEAL / BYTES_PER_SECTOR;
pub const SECTORS_PER_PRICING: usize = BYTES_PER_PRICING / BYTES_PER_SECTOR;
pub const SECTORS_PER_MAX_MINING_RANGE: usize = BYTES_PER_MAX_MINING_RANGE / BYTES_PER_SECTOR;

pub const SEALS_PER_LOAD: usize = BYTES_PER_LOAD / BYTES_PER_SEAL;
