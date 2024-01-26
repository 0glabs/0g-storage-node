use super::FileID;
use crate::{Config, SegmentInfo};
use anyhow::{bail, Result};
use hashlink::LinkedHashMap;
use shared_types::{bytes_to_chunks, ChunkArray, DataRoot, FileProof, Transaction, CHUNK_SIZE};
use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, Instant};

/// Used to cache chunks in memory pool and persist into db once log entry
/// retrieved from blockchain.
pub struct MemoryCachedFile {
    pub id: FileID,
    pub chunks_per_segment: usize,
    /// Window to control the cache of each file
    pub segments: HashMap<usize, (ChunkArray, FileProof)>,
    /// Total number of chunks for the cache file, which is updated from log entry.
    pub total_chunks: usize,
    /// Used for garbage collection. It is updated when new segment uploaded.
    expired_at: Instant,
    /// Number of chunks that's currently cached for this file
    pub cached_chunk_num: usize,
}

impl MemoryCachedFile {
    fn new(root: DataRoot, timeout: Duration, chunks_per_segment: usize) -> Self {
        MemoryCachedFile {
            id: FileID {
                root,
                tx_id: Default::default(),
            },
            chunks_per_segment,
            segments: HashMap::default(),
            total_chunks: 0,
            expired_at: Instant::now().add(timeout),
            cached_chunk_num: 0,
        }
    }

    /// Updates file with transaction once log entry retrieved from blockchain.
    /// So that, write memory cached segments into database.
    pub fn update_with_tx(&mut self, tx: &Transaction) {
        self.total_chunks = bytes_to_chunks(tx.size as usize);
        self.id.tx_id = tx.id();
    }

    fn update_expiration_time(&mut self, timeout: Duration) {
        self.expired_at = Instant::now().add(timeout);
    }

    #[allow(unused)]
    fn is_completed(&self) -> bool {
        self.total_chunks > 0 && self.cached_chunk_num >= self.total_chunks
    }

    fn should_flush(&self) -> bool {
        self.total_chunks > 0 && self.cached_chunk_num > 0
    }
}

/// ChunkPoolCache is used to cache small files that log entry not retrieved
/// from L1 blockchain yet.
pub struct ChunkPoolCache {
    config: Config,
    /// All cached files.
    /// Note, file root is used as key instead of `tx_seq`, since log entry
    /// not retrieved yet.
    files: LinkedHashMap<DataRoot, MemoryCachedFile>,
    /// Total number of chunks that cached in the memory pool.
    pub total_chunks: usize,
}

impl ChunkPoolCache {
    pub fn new(config: Config) -> Self {
        ChunkPoolCache {
            config,
            files: LinkedHashMap::default(),
            total_chunks: 0,
        }
    }

    pub fn get_file(&self, root: &DataRoot) -> Option<&MemoryCachedFile> {
        self.files.get(root)
    }

    #[allow(unused)]
    pub fn get_file_mut(&mut self, root: &DataRoot) -> Option<&mut MemoryCachedFile> {
        self.files.get_mut(root)
    }

    pub fn remove_file(&mut self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let file = self.files.remove(root)?;
        self.update_total_chunks_when_remove_file(&file);
        Some(file)
    }

    /// Remove files that no new segment uploaded for a long time.
    ///
    /// Note, when log sync delayed, files may be also garbage collected if the
    /// entire file uploaded. Because, it is hard to check if log sync delayed
    /// or user upload an invalid file, e.g. for attack purpose.
    ///
    /// Once garbage collected, user could simply upload the entire file again,
    /// which is fast enough due to small file size.
    fn garbage_collect(&mut self) {
        let now = Instant::now();

        while let Some((_, file)) = self.files.front() {
            if file.expired_at > now {
                return;
            }

            if let Some((r, f)) = self.files.pop_front() {
                self.update_total_chunks_when_remove_file(&f);
                debug!("Garbage collected for file {}", r);
            }
        }
    }

    fn update_total_chunks_when_remove_file(&mut self, file: &MemoryCachedFile) {
        assert!(self.total_chunks >= file.cached_chunk_num);
        self.total_chunks -= file.cached_chunk_num;
    }

    /// Caches the specified segment in memory.
    ///
    /// Returns if there are cached segments and log entry also retrieved.
    pub fn cache_segment(&mut self, seg_info: SegmentInfo) -> Result<bool> {
        // always GC at first
        self.garbage_collect();

        let file = self.files.entry(seg_info.root).or_insert_with(|| {
            MemoryCachedFile::new(
                seg_info.root,
                self.config.expiration_time(),
                seg_info.chunks_per_segment,
            )
        });

        // Segment already cached in memory. Directly return OK
        if file.segments.contains_key(&seg_info.seg_index) {
            return Ok(file.should_flush());
        }

        // Otherwise, just cache segment in memory
        let num_chunks = seg_info.seg_data.len() / CHUNK_SIZE;

        // Limits the cached chunks in the memory pool.
        if self.total_chunks + num_chunks > self.config.max_cached_chunks_all {
            bail!(
                "exceeds the maximum cached chunks of whole pool: {}",
                self.config.max_cached_chunks_all
            );
        }

        // Cache segment and update the counter for cached chunks.
        self.total_chunks += num_chunks;
        file.cached_chunk_num += num_chunks;
        file.update_expiration_time(self.config.expiration_time());
        file.segments.insert(seg_info.seg_index, seg_info.into());

        Ok(file.should_flush())
    }
}
