use crate::Config;
use network::types::SignedAnnounceFile;
use network::PeerId;
use parking_lot::Mutex;
use priority_queue::PriorityQueue;
use rand::seq::IteratorRandom;
use shared_types::{timestamp_now, TxID};
use std::cmp::Reverse;
use std::collections::HashMap;
use storage::config::ShardConfig;

/// Caches limited announcements of specified file from different peers.
struct AnnouncementCache {
    /// Maximum number of announcements in cache.
    capacity: usize,

    /// Timeout in seconds to expire the cached announcement.
    /// This is because file may be removed from the announced
    /// storage node.
    timeout_secs: u32,

    /// All cached announcements that mapped from peer id to announcement.
    /// Note, only cache the latest announcement for each peer.
    items: HashMap<PeerId, SignedAnnounceFile>,

    /// All announcements are prioritized by timestamp.
    /// The top element is the oldest announcement.
    priorities: PriorityQueue<PeerId, Reverse<u32>>,
}

impl AnnouncementCache {
    fn new(capacity: usize, timeout_secs: u32) -> Self {
        assert!(capacity > 0);

        AnnouncementCache {
            capacity,
            timeout_secs,
            items: Default::default(),
            priorities: Default::default(),
        }
    }

    /// Returns the priority of the oldest announcement if any.
    fn peek_priority(&self) -> Option<Reverse<u32>> {
        let (_, &Reverse(ts)) = self.priorities.peek()?;
        Some(Reverse(ts))
    }

    /// Removes the oldest announcement if any.
    fn pop(&mut self) -> Option<SignedAnnounceFile> {
        let (peer_id, _) = self.priorities.pop()?;
        self.items.remove(&peer_id)
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    /// Garbage collects expired announcements.
    fn garbage_collect(&mut self) -> usize {
        let mut collected = 0;
        let now = timestamp_now();

        while let Some((_, &Reverse(ts))) = self.priorities.peek() {
            if ts + self.timeout_secs > now {
                break;
            }

            self.pop();
            collected += 1;
        }

        collected
    }

    fn do_insert_or_update(&mut self, announcement: SignedAnnounceFile) {
        let peer_id = announcement.peer_id.clone().into();
        self.priorities
            .push(peer_id, Reverse(announcement.timestamp));
        self.items.insert(peer_id, announcement);
    }

    /// Insert the specified `announcement` into cache.
    fn insert(&mut self, announcement: SignedAnnounceFile) {
        self.garbage_collect();

        let peer_id = announcement.peer_id.clone().into();

        if let Some(existing) = self.items.get(&peer_id) {
            // ignore older announcement
            if announcement.timestamp <= existing.timestamp {
                return;
            }
        }

        // insert or update
        self.do_insert_or_update(announcement);

        // remove oldest one if capacity exceeded
        if self.items.len() > self.capacity {
            self.pop();
        }
    }

    /// Randomly pick an announcement if any.
    fn random(&mut self) -> (Option<SignedAnnounceFile>, usize) {
        let collected = self.garbage_collect();

        // all announcements garbage collected
        if self.items.is_empty() {
            return (None, collected);
        }

        let choosed = self
            .items
            .iter()
            .choose(&mut rand::thread_rng())
            .map(|(_, item)| item.clone());

        (choosed, collected)
    }

    /// Returns all announcements.
    fn all(&mut self) -> (Vec<SignedAnnounceFile>, usize) {
        let collected = self.garbage_collect();
        let result = self.items.values().cloned().collect();
        (result, collected)
    }

    /// Removes announcement for the specified `peer_id` if any.
    fn remove(&mut self, peer_id: &PeerId) -> Option<SignedAnnounceFile> {
        self.priorities.remove(peer_id)?;
        self.items.remove(peer_id)
    }
}

/// Caches announcements for different files.
struct FileCache {
    /// Cache configuration.
    config: Config,

    /// Total number of announcements cached for all files.
    total_announcements: usize,

    /// All cached files that mapped from `tx_id` to `AnnouncementCache`.
    files: HashMap<TxID, AnnouncementCache>,

    /// All files are prioritized by timestamp.
    /// The top element is the `AnnouncementCache` that has the oldest announcement.
    priorities: PriorityQueue<TxID, Reverse<u32>>,
}

impl FileCache {
    fn new(config: Config) -> Self {
        FileCache {
            config,
            total_announcements: 0,
            files: Default::default(),
            priorities: Default::default(),
        }
    }

    /// Insert the specified `announcement` into cache.
    fn insert(&mut self, announcement: SignedAnnounceFile) {
        let tx_id = announcement.tx_id;

        let item = self.files.entry(tx_id).or_insert_with(|| {
            AnnouncementCache::new(
                self.config.max_entries_per_file,
                self.config.entry_expiration_time_secs,
            )
        });

        assert!(self.total_announcements >= item.len());
        self.total_announcements -= item.len();

        item.insert(announcement);

        if let Some(priority) = item.peek_priority() {
            self.priorities.push(tx_id, priority);
        }

        self.total_announcements += item.len();
        if self.total_announcements > self.config.max_entries_total {
            self.pop();
        }
    }

    /// Removes the oldest file announcement.
    fn pop(&mut self) -> Option<SignedAnnounceFile> {
        let (&tx_id, _) = self.priorities.peek()?;
        let item = self.files.get_mut(&tx_id)?;

        let result = item.pop()?;

        self.update_on_announcement_cache_changed(&tx_id, 1);

        Some(result)
    }

    /// Randomly pick a announcement of specified file by `tx_id`.
    fn random(&mut self, tx_id: TxID) -> Option<SignedAnnounceFile> {
        let item = self.files.get_mut(&tx_id)?;
        let (result, collected) = item.random();
        self.update_on_announcement_cache_changed(&tx_id, collected);
        result
    }

    fn update_on_announcement_cache_changed(&mut self, tx_id: &TxID, removed: usize) {
        if removed == 0 {
            return;
        }

        assert!(self.total_announcements >= removed);
        self.total_announcements -= removed;

        let item = match self.files.get_mut(tx_id) {
            Some(v) => v,
            None => return,
        };

        if let Some(priority) = item.peek_priority() {
            // update priority if changed
            self.priorities.change_priority(tx_id, priority);
        } else {
            // remove entry if empty
            self.files.remove(tx_id);
            self.priorities.remove(tx_id);
        }
    }

    /// Returns all the announcements of specified file by `tx_id`.
    fn all(&mut self, tx_id: TxID) -> Option<Vec<SignedAnnounceFile>> {
        let item = self.files.get_mut(&tx_id)?;
        let (result, collected) = item.all();
        self.update_on_announcement_cache_changed(&tx_id, collected);
        Some(result)
    }

    /// Removes the announcement of specified file by `tx_id` and `peer_id`.
    fn remove(&mut self, tx_id: &TxID, peer_id: &PeerId) -> Option<SignedAnnounceFile> {
        let item = self.files.get_mut(tx_id)?;
        let result = item.remove(peer_id)?;
        self.update_on_announcement_cache_changed(tx_id, 1);
        Some(result)
    }
}

#[derive(Default)]
pub struct PeerShardConfigCache {
    peers: HashMap<PeerId, ShardConfig>,
}

impl PeerShardConfigCache {
    pub fn insert(&mut self, peer: PeerId, config: ShardConfig) -> Option<ShardConfig> {
        self.peers.insert(peer, config)
    }

    pub fn get(&self, peer: &PeerId) -> Option<ShardConfig> {
        self.peers.get(peer).cloned()
    }
}

pub struct FileLocationCache {
    cache: Mutex<FileCache>,
    peer_cache: Mutex<PeerShardConfigCache>,
}

impl Default for FileLocationCache {
    fn default() -> Self {
        FileLocationCache {
            cache: Mutex::new(FileCache::new(Default::default())),
            peer_cache: Mutex::new(Default::default()),
        }
    }
}

impl FileLocationCache {
    pub fn new(config: Config) -> Self {
        FileLocationCache {
            cache: Mutex::new(FileCache::new(config)),
            peer_cache: Mutex::new(Default::default()),
        }
    }

    pub fn insert(&self, announcement: SignedAnnounceFile) {
        let peer_id = *announcement.peer_id;
        // FIXME: Check validity.
        let shard_config = ShardConfig {
            shard_id: announcement.shard_id,
            num_shard: announcement.num_shard,
        };
        self.cache.lock().insert(announcement);
        self.insert_peer_config(peer_id, shard_config);
    }

    pub fn get_one(&self, tx_id: TxID) -> Option<SignedAnnounceFile> {
        self.cache.lock().random(tx_id)
    }

    pub fn get_all(&self, tx_id: TxID) -> Vec<SignedAnnounceFile> {
        self.cache.lock().all(tx_id).unwrap_or_default()
    }

    pub fn remove(&self, tx_id: &TxID, peer_id: &PeerId) -> Option<SignedAnnounceFile> {
        self.cache.lock().remove(tx_id, peer_id)
    }

    /// TODO: Trigger chunk_pool/sync to reconstruct if it changes?
    pub fn insert_peer_config(
        &self,
        peer: PeerId,
        shard_config: ShardConfig,
    ) -> Option<ShardConfig> {
        self.peer_cache.lock().insert(peer, shard_config)
    }

    pub fn get_peer_config(&self, peer: &PeerId) -> Option<ShardConfig> {
        self.peer_cache.lock().get(peer)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use network::{types::SignedAnnounceFile, PeerId};
    use shared_types::{timestamp_now, TxID};

    use crate::{test_util::AnnounceFileBuilder, Config};

    use super::{AnnouncementCache, FileCache};

    fn create_file(peer_id: Option<PeerId>, timestamp: u32) -> SignedAnnounceFile {
        let builder = AnnounceFileBuilder::default().with_timestamp(timestamp);
        if let Some(id) = peer_id {
            builder.with_peer_id(id).build()
        } else {
            builder.build()
        }
    }

    #[test]
    fn test_annoucement_cache_peek_priority() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        // empty by default
        assert_eq!(cache.peek_priority(), None);

        // one file with timestamp `now - 5`
        let t1 = now - 5;
        cache.insert(create_file(None, t1));
        assert_eq!(cache.peek_priority(), Some(Reverse(t1)));

        // newly file with timestamp `now - 4`
        let t2 = now - 4;
        cache.insert(create_file(None, t2));
        assert_eq!(cache.peek_priority(), Some(Reverse(t1)));

        // old file with timestamp `now - 6`
        let t3 = now - 6;
        cache.insert(create_file(None, t3));
        assert_eq!(cache.peek_priority(), Some(Reverse(t3)));
    }

    #[test]
    fn test_annoucement_cache_pop_len() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        // empty by default
        assert_eq!(cache.pop(), None);
        assert_eq!(cache.len(), 0);

        cache.insert(create_file(None, now - 2));
        cache.insert(create_file(None, now - 3));
        cache.insert(create_file(None, now - 1));
        assert_eq!(cache.len(), 3);

        // pop from oldest to newest
        assert_eq!(cache.pop().unwrap().timestamp, now - 3);
        assert_eq!(cache.pop().unwrap().timestamp, now - 2);
        assert_eq!(cache.pop().unwrap().timestamp, now - 1);
        assert_eq!(cache.pop(), None);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_annoucement_cache_garbage_collect() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        assert_eq!(cache.garbage_collect(), 0);

        cache.do_insert_or_update(create_file(None, now - 5000));
        cache.do_insert_or_update(create_file(None, now - 5001));
        cache.do_insert_or_update(create_file(None, now - 2000));
        cache.do_insert_or_update(create_file(None, now + 10));

        // gc for expired only
        assert_eq!(cache.garbage_collect(), 2);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 2000)));
    }

    #[test]
    fn test_annoucement_cache_insert_gc() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        // prepare expired items
        cache.do_insert_or_update(create_file(None, now - 5000));
        cache.do_insert_or_update(create_file(None, now - 5001));

        // insert with gc
        cache.insert(create_file(None, now - 1));

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 1)));
    }

    #[test]
    fn test_annoucement_cache_insert_ignore_older() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        let peer_id = PeerId::random();

        // insert `now - 2`
        cache.insert(create_file(Some(peer_id), now - 2));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 2)));
        assert_eq!(cache.len(), 1);

        // ignore for older announcement of the same peer
        cache.insert(create_file(Some(peer_id), now - 3));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 2)));
        assert_eq!(cache.len(), 1);

        // however, older announcement allowed from other peer
        cache.insert(create_file(None, now - 3));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 3)));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_annoucement_cache_insert_overwrite() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        let peer_id = PeerId::random();

        // insert `now - 2`
        cache.insert(create_file(Some(peer_id), now - 2));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 2)));
        assert_eq!(cache.len(), 1);

        // overwrite with newly item
        cache.insert(create_file(Some(peer_id), now - 1));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 1)));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_annoucement_cache_insert_cap_exceeded() {
        let mut cache = AnnouncementCache::new(3, 3600);
        let now = timestamp_now();

        cache.insert(create_file(None, now - 2));
        cache.insert(create_file(None, now - 3));
        cache.insert(create_file(None, now - 4));

        // oldest `now - 5` will be removed
        cache.insert(create_file(None, now - 5));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 4)));
        assert_eq!(cache.len(), 3);

        // oldest `now - 4` will be removed
        cache.insert(create_file(None, now - 1));
        assert_eq!(cache.peek_priority(), Some(Reverse(now - 3)));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_annoucement_cache_random() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        assert_eq!(cache.random().0, None);

        cache.insert(create_file(None, now - 1));
        assert_eq!(cache.random().0.unwrap().timestamp, now - 1);

        cache.insert(create_file(None, now - 2));
        cache.insert(create_file(None, now - 3));
        let picked = cache.random().0.unwrap().timestamp;
        assert!(picked >= now - 3 && picked < now);
    }

    #[test]
    fn test_annoucement_cache_all() {
        let mut cache = AnnouncementCache::new(100, 3600);
        let now = timestamp_now();

        assert_eq!(cache.all().0, vec![]);

        cache.insert(create_file(None, now - 1));
        cache.insert(create_file(None, now - 2));
        cache.insert(create_file(None, now - 3));

        assert_all_files(cache.all().0, vec![now - 3, now - 2, now - 1])
    }

    fn assert_all_files(files: Vec<SignedAnnounceFile>, sorted_timestamps: Vec<u32>) {
        let mut timestamps: Vec<u32> = files.iter().map(|f| f.timestamp).collect();
        timestamps.sort();
        assert_eq!(timestamps, sorted_timestamps);
    }

    fn create_file_cache(total_entries: usize, file_entries: usize, timeout: u32) -> FileCache {
        FileCache::new(Config {
            max_entries_total: total_entries,
            max_entries_per_file: file_entries,
            entry_expiration_time_secs: timeout,
        })
    }

    fn create_file_2(tx_id: TxID, peer_id: PeerId, timestamp: u32) -> SignedAnnounceFile {
        AnnounceFileBuilder::default()
            .with_tx_id(tx_id)
            .with_peer_id(peer_id)
            .with_timestamp(timestamp)
            .build()
    }

    fn assert_file(file: &SignedAnnounceFile, tx_id: TxID, peer_id: PeerId, timestamp: u32) {
        assert_eq!(file.tx_id, tx_id);
        assert_eq!(PeerId::from(file.peer_id.clone()), peer_id);
        assert_eq!(file.timestamp, timestamp);
    }

    #[test]
    fn test_file_cache_insert_pop() {
        let mut cache = create_file_cache(100, 3, 3600);
        let now = timestamp_now();

        assert_eq!(cache.total_announcements, 0);

        let peer1 = PeerId::random();
        let peer2 = PeerId::random();
        let tx1 = TxID::random_hash(1);
        let tx2 = TxID::random_hash(2);

        cache.insert(create_file_2(tx1, peer1, now - 1));
        assert_eq!(cache.total_announcements, 1);
        cache.insert(create_file_2(tx2, peer1, now - 2));
        assert_eq!(cache.total_announcements, 2);
        cache.insert(create_file_2(tx1, peer2, now - 3));
        assert_eq!(cache.total_announcements, 3);

        assert_file(&cache.pop().unwrap(), tx1, peer2, now - 3);
        assert_eq!(cache.total_announcements, 2);
        assert_file(&cache.pop().unwrap(), tx2, peer1, now - 2);
        assert_eq!(cache.total_announcements, 1);
        assert_file(&cache.pop().unwrap(), tx1, peer1, now - 1);
        assert_eq!(cache.total_announcements, 0);
        assert_eq!(cache.pop(), None);
    }

    #[test]
    fn test_file_cache_insert_cap_exceeded() {
        let mut cache = create_file_cache(3, 3, 3600);
        let now = timestamp_now();

        let tx1 = TxID::random_hash(1);
        cache.insert(create_file_2(tx1, PeerId::random(), now - 7));
        cache.insert(create_file_2(tx1, PeerId::random(), now - 8));
        cache.insert(create_file_2(tx1, PeerId::random(), now - 9));
        assert_eq!(cache.total_announcements, 3);

        // insert more files and cause to max entries limited
        let tx2 = TxID::random_hash(2);
        cache.insert(create_file_2(tx2, PeerId::random(), now - 1));
        assert_all_files(cache.all(tx1).unwrap_or_default(), vec![now - 8, now - 7]);
        cache.insert(create_file_2(tx2, PeerId::random(), now - 2));
        assert_all_files(cache.all(tx1).unwrap_or_default(), vec![now - 7]);
        cache.insert(create_file_2(tx2, PeerId::random(), now - 3));
        assert_all_files(cache.all(tx1).unwrap_or_default(), vec![]);

        assert_all_files(
            cache.all(tx2).unwrap_or_default(),
            vec![now - 3, now - 2, now - 1],
        );
    }
}
