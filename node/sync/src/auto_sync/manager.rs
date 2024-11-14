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
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::{Config, SyncSender};

use super::{
    batcher_random::RandomBatcher,
    batcher_serial::SerialBatcher,
    historical_tx_writer::HistoricalTxWriter,
    sync_store::{Queue, SyncStore},
};

pub struct AutoSyncManager {
    pub serial: Option<SerialBatcher>,
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
        let (new_file_send, new_file_recv) = unbounded_channel();
        let sync_store = if config.neighbors_only {
            // use v2 db to avoid reading v1 files that announced from the whole network instead of neighbors
            Arc::new(SyncStore::new_with_name(
                store.clone(),
                "pendingv2",
                "readyv2",
            ))
        } else {
            Arc::new(SyncStore::new(store.clone()))
        };
        let catched_up = Arc::new(AtomicBool::new(false));

        // handle new file
        executor.spawn(
            Self::handle_new_file(new_file_recv, sync_store.clone()),
            "auto_sync_handle_new_file",
        );

        // sync in sequence
        let serial = if config.neighbors_only {
            None
        } else {
            let serial =
                SerialBatcher::new(config, store.clone(), sync_send.clone(), sync_store.clone())
                    .await?;
            executor.spawn(
                serial
                    .clone()
                    .start(file_announcement_recv, log_sync_recv, catched_up.clone()),
                "auto_sync_serial",
            );

            Some(serial)
        };

        // sync randomly
        let random = RandomBatcher::new(config, store.clone(), sync_send.clone(), sync_store);
        executor.spawn(random.clone().start(catched_up.clone()), "auto_sync_random");

        // handle on catched up notification
        executor.spawn(
            Self::listen_catch_up(catch_up_end_recv, catched_up.clone()),
            "auto_sync_wait_for_catchup",
        );

        // sync randomly for files without NewFile announcement
        if config.neighbors_only {
            let historical_sync_store = Arc::new(SyncStore::new_with_name(
                store.clone(),
                "pendingv2_historical",
                "readyv2_historical",
            ));

            let writer =
                HistoricalTxWriter::new(config, store.clone(), historical_sync_store.clone())
                    .await?;
            executor.spawn(writer.start(), "auto_sync_historical_writer");

            let random_historical =
                RandomBatcher::new(config, store, sync_send, historical_sync_store);
            executor.spawn(
                random_historical.start(catched_up.clone()),
                "auto_sync_random_historical",
            );
        }

        Ok(Self {
            serial,
            random,
            file_announcement_send,
            new_file_send,
            catched_up,
        })
    }

    async fn handle_new_file(
        mut new_file_recv: UnboundedReceiver<u64>,
        sync_store: Arc<SyncStore>,
    ) {
        while let Some(tx_seq) = new_file_recv.recv().await {
            if let Err(err) = sync_store.insert(tx_seq, Queue::Ready).await {
                warn!(?err, %tx_seq, "Failed to insert new file to ready queue");
            }
        }
    }

    async fn listen_catch_up(
        catch_up_end_recv: oneshot::Receiver<()>,
        catched_up: Arc<AtomicBool>,
    ) {
        if catch_up_end_recv.await.is_ok() {
            info!("log entry catched up");
            catched_up.store(true, Ordering::Relaxed);
        }
    }
}
