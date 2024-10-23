use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use anyhow::Result;
use log_entry_sync::LogSyncEvent;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::{
    broadcast,
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot,
};

use crate::{Config, SyncSender};

use super::{
    batcher_random::RandomBatcher,
    batcher_serial::SerialBatcher,
    sync_store::{Queue, SyncStore},
};

pub struct AutoSyncManager {
    pub serial: SerialBatcher,
    pub random: RandomBatcher,
    pub file_announcement_send: UnboundedSender<u64>,
    pub new_file_send: UnboundedSender<u64>,
    pub catched_up: Arc<AtomicBool>,
}

impl AutoSyncManager {
    pub async fn spawn(
        config: Config,
        executor: &TaskExecutor,
        store: Store,
        sync_send: SyncSender,
        log_sync_recv: broadcast::Receiver<LogSyncEvent>,
        catch_up_end_recv: oneshot::Receiver<()>,
    ) -> Result<Self> {
        let (file_announcement_send, file_announcement_recv) = unbounded_channel();
        let (new_file_send, mut new_file_recv) = unbounded_channel();
        let sync_store = Arc::new(SyncStore::new(store.clone()));
        let catched_up = Arc::new(AtomicBool::new(false));

        // handle new file
        let sync_store_cloned = sync_store.clone();
        executor.spawn(
            async move {
                while let Some(tx_seq) = new_file_recv.recv().await {
                    if let Err(err) = sync_store_cloned.insert(tx_seq, Queue::Ready).await {
                        warn!(?err, %tx_seq, "Failed to insert new file to ready queue");
                    }
                }
            },
            "auto_sync_handle_new_file",
        );

        // sync in sequence
        let serial =
            SerialBatcher::new(config, store.clone(), sync_send.clone(), sync_store.clone())
                .await?;
        executor.spawn(
            serial
                .clone()
                .start(file_announcement_recv, log_sync_recv, catched_up.clone()),
            "auto_sync_serial",
        );

        // sync randomly
        let random = RandomBatcher::new(config, store, sync_send, sync_store);
        executor.spawn(random.clone().start(catched_up.clone()), "auto_sync_random");

        // handle on catched up notification
        let catched_up_cloned = catched_up.clone();
        executor.spawn(
            async move {
                if catch_up_end_recv.await.is_ok() {
                    info!("log entry catched up");
                    catched_up_cloned.store(true, Ordering::Relaxed);
                }
            },
            "auto_sync_wait_for_catchup",
        );

        Ok(Self {
            serial,
            random,
            file_announcement_send,
            new_file_send,
            catched_up,
        })
    }
}
