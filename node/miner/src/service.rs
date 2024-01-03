use crate::sealer::Sealer;
use crate::submitter::Submitter;
use crate::{config::MinerConfig, mine::PoraService, watcher::MineContextWatcher};
use network::NetworkMessage;
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, RwLock};

#[derive(Clone, Debug)]
pub enum MinerMessage {
    /// Enable / Disable Mining
    ToggleMining(bool),

    /// Change mining range
    SetStartPosition(Option<u64>),
    SetEndPosition(Option<u64>),
}

pub struct MineService;

impl MineService {
    pub async fn spawn(
        executor: task_executor::TaskExecutor,
        _network_send: mpsc::UnboundedSender<NetworkMessage>,
        config: MinerConfig,
        store: Arc<RwLock<dyn Store>>,
    ) -> Result<broadcast::Sender<MinerMessage>, String> {
        let provider = Arc::new(config.make_provider().await?);

        let (msg_send, msg_recv) = broadcast::channel(1024);

        let mine_context_receiver = MineContextWatcher::spawn(
            executor.clone(),
            msg_recv.resubscribe(),
            provider.clone(),
            &config,
        );

        let mine_answer_receiver = PoraService::spawn(
            executor.clone(),
            msg_recv.resubscribe(),
            mine_context_receiver,
            Arc::new(store.clone()),
            &config,
        );

        Submitter::spawn(
            executor.clone(),
            mine_answer_receiver,
            provider.clone(),
            store.clone(),
            &config,
        );

        Sealer::spawn(executor, provider, store, &config);

        debug!("Starting miner service");

        Ok(msg_send)
    }
}
