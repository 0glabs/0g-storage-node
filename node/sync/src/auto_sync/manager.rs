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

use super::{batcher_random::RandomBatcher, batcher_serial::SerialBatcher, sync_store::SyncStore};

pub struct AutoSyncManager {
    pub serial: SerialBatcher,
    pub random: RandomBatcher,
    pub file_announcement_send: UnboundedSender<u64>,
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
        let (send, recv) = unbounded_channel();
        let sync_store = Arc::new(SyncStore::new(store.clone()));
        let catched_up = Arc::new(AtomicBool::new(false));

        // sync in sequence
        let serial =
            SerialBatcher::new(config, store.clone(), sync_send.clone(), sync_store.clone())
                .await?;
        executor.spawn(
            serial
                .clone()
                .start(recv, log_sync_recv, catched_up.clone()),
            "auto_sync_serial",
        );

        // sync randomly
        let random = RandomBatcher::new(config, store.clone(), sync_send.clone(), sync_store);
        executor.spawn(random.clone().start(catched_up.clone()), "auto_sync_random");

        // handle on catched up notification
        executor.spawn(
            async move {
                catch_up_end_recv.await.expect("Catch up sender dropped");
                info!("log entry catched up");
                catched_up.store(true, Ordering::Relaxed);
            },
            "auto_sync_wait_for_catchup",
        );

        Ok(Self {
            serial,
            random,
            file_announcement_send: send,
        })
    }
}
