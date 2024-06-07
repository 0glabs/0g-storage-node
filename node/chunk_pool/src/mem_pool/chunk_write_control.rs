use super::FileID;
use crate::Config;
use anyhow::{bail, Result};
use shared_types::DataRoot;
use std::collections::HashMap;
use storage_async::ShardConfig;

/// The segment status in sliding window
#[derive(PartialEq, Eq, Debug)]
enum SlotStatus {
    Writing,  // segment in writing
    Finished, // segment uploaded in store
}

/// Sliding window is used to control the concurrent uploading process of a file.
/// Bounded window allows segments to be uploaded concurrenly, while having a capacity
/// limit on writing threads per file. Meanwhile, the left_boundary field records
/// how many segments have been uploaded.
struct CtrlWindow {
    #[allow(unused)]
    size: usize,
    tx_start_index: usize,
    shard_config: ShardConfig,
    left_boundary: usize,
    slots: HashMap<usize, SlotStatus>,
}

impl CtrlWindow {
    fn new(size: usize, shard_config: ShardConfig, tx_start_index: usize) -> Self {
        CtrlWindow {
            size,
            tx_start_index,
            shard_config,
            left_boundary: shard_config.next_segment_index(0, tx_start_index)
                % shard_config.num_shard,
            slots: HashMap::default(),
        }
    }

    /// Check if the specified slot by `index` has been already uploaded.
    /// Note, this function do not check about the right boundary.
    fn check_duplicate(&self, index: usize) -> bool {
        index < self.left_boundary || self.slots.contains_key(&index)
    }

    /// Should call check_duplicate and handle the duplicated case before calling this function.
    /// This function assumes that there are no duplicate slots in the window.
    fn start_writing(&mut self, index: usize) -> Result<()> {
        assert!(index >= self.left_boundary);

        // if index >= self.left_boundary + self.size {
        //     bail!(
        //         "index exceeds window limit, index = {}, left_boundary = {}, window_size = {}",
        //         index,
        //         self.left_boundary,
        //         self.size
        //     );
        // }

        assert!(!self.slots.contains_key(&index));
        self.slots.insert(index, SlotStatus::Writing);

        Ok(())
    }

    fn rollback_writing(&mut self, index: usize) {
        let slot_status = self.slots.remove(&index);
        assert_eq!(slot_status, Some(SlotStatus::Writing));
    }

    fn finish_writing(&mut self, index: usize) {
        let old_status = self.slots.insert(index, SlotStatus::Finished);
        assert_eq!(old_status, Some(SlotStatus::Writing));

        // move forward if leftmost slot completed
        let mut left_boundary = self.left_boundary;
        while let Some(&SlotStatus::Finished) = self.slots.get(&left_boundary) {
            self.slots.remove(&left_boundary);
            // Handle shard_config change.
            left_boundary = self
                .shard_config
                .next_segment_index(left_boundary, self.tx_start_index);
        }

        self.left_boundary = left_boundary;
    }
}

/// To track the file uploading progress.
pub struct FileWriteCtrl {
    pub id: FileID,
    total_segments: usize,
    window: CtrlWindow,
}

impl FileWriteCtrl {
    fn new(
        id: FileID,
        total_segments: usize,
        window_size: usize,
        shard_config: ShardConfig,
        tx_start_index: usize,
    ) -> Self {
        FileWriteCtrl {
            id,
            total_segments,
            window: CtrlWindow::new(window_size, shard_config, tx_start_index),
        }
    }

    pub fn uploaded_seg_num(&self) -> usize {
        self.window.left_boundary
    }
}

/// ChunkPoolWriteCtrl is used to track uploading progress for all files,
/// and limits the maximum number of threads to write segments into store.
pub struct ChunkPoolWriteCtrl {
    config: Config,
    /// Windows to control writing processes of files
    files: HashMap<DataRoot, FileWriteCtrl>,
    /// Total number of threads that are writing chunks into store.
    pub total_writings: usize,
}

impl ChunkPoolWriteCtrl {
    pub fn new(config: Config) -> Self {
        ChunkPoolWriteCtrl {
            files: HashMap::default(),
            total_writings: 0,
            config,
        }
    }

    pub fn get_file(&self, root: &DataRoot) -> Option<&FileWriteCtrl> {
        self.files.get(root)
    }

    pub fn remove_file(&mut self, root: &DataRoot) -> Option<FileWriteCtrl> {
        self.files.remove(root)
    }

    pub fn write_segment(
        &mut self,
        id: FileID,
        seg_index: usize,
        total_segments: usize,
        tx_start_index: usize,
    ) -> Result<()> {
        let file_ctrl = self.files.entry(id.root).or_insert_with(|| {
            FileWriteCtrl::new(
                id,
                total_segments,
                self.config.write_window_size,
                self.config.shard_config,
                tx_start_index,
            )
        });

        // ensure the tx_id not changed during file uploading
        if file_ctrl.id != id {
            self.files.remove(&id.root);
            bail!("Transaction reverted when uploading segments, please try again");
        }

        if file_ctrl.total_segments != total_segments {
            bail!(
                "file size in segment doesn't match with file size declared in previous segment. Previous total segments:{}, current total segments:{}s",
                file_ctrl.total_segments,
                total_segments
            );
        }

        // Segment already uploaded.
        if file_ctrl.window.check_duplicate(seg_index) {
            bail!("segment has already been uploaded or is being uploaded");
        }

        // Limits the number of writing threads.
        if self.total_writings >= self.config.max_writings {
            bail!("too many data writing: {}", self.config.max_writings);
        }

        file_ctrl.window.start_writing(seg_index)?;

        self.total_writings += 1;

        Ok(())
    }

    pub fn on_write_succeeded(&mut self, root: &DataRoot, seg_index: usize) -> bool {
        let file_ctrl = match self.files.get_mut(root) {
            Some(w) => w,
            None => return false,
        };

        file_ctrl.window.finish_writing(seg_index);

        assert!(self.total_writings > 0);
        self.total_writings -= 1;

        debug!(
            "Succeeded to write segment, root={}, seg_index={}, total_writings={}",
            root, seg_index, self.total_writings
        );

        // All chunks of file written into store.
        file_ctrl.window.left_boundary + self.config.shard_config.num_shard
            > file_ctrl.total_segments
    }

    pub fn on_write_failed(&mut self, root: &DataRoot, seg_index: usize) {
        let file_ctrl = match self.files.get_mut(root) {
            Some(w) => w,
            None => return,
        };

        //Rollback the segment status if failed
        file_ctrl.window.rollback_writing(seg_index);

        assert!(self.total_writings > 0);
        self.total_writings -= 1;
    }

    pub fn update_shard_config(&mut self, shard_config: ShardConfig) {
        self.config.shard_config = shard_config;
        for file_ctrl in self.files.values_mut() {
            file_ctrl.window.shard_config = shard_config;
        }
    }
}
