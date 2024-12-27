#![allow(unused)]

use contract_interface::{zgs_flow::MineContext, PoraMine, WorkerContext, ZgsFlow};
use ethereum_types::{Address, H256, U256};
use ethers::{
    contract::Contract,
    providers::{JsonRpcClient, Middleware, Provider, StreamExt},
    types::BlockId,
};
use task_executor::TaskExecutor;
use tokio::{
    sync::{broadcast, mpsc},
    time::{sleep, Instant, Sleep},
    try_join,
};

use crate::{config::MineServiceMiddleware, mine::PoraPuzzle, MinerConfig, MinerMessage};
use ethers::prelude::{Http, RetryClient};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{ops::DerefMut, str::FromStr};

pub type MineContextMessage = Option<PoraPuzzle>;

lazy_static! {
    pub static ref EMPTY_HASH: H256 =
        H256::from_str("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").unwrap();
}

pub struct MineContextWatcher {
    provider: Arc<Provider<RetryClient<Http>>>,
    flow_contract: ZgsFlow<Provider<RetryClient<Http>>>,
    mine_contract: PoraMine<Provider<RetryClient<Http>>>,

    mine_context_sender: broadcast::Sender<MineContextMessage>,
    last_report: MineContextMessage,
    query_interval: Duration,
    miner_id: H256,

    msg_recv: broadcast::Receiver<MinerMessage>,
}

impl MineContextWatcher {
    pub fn spawn(
        executor: TaskExecutor,
        msg_recv: broadcast::Receiver<MinerMessage>,
        provider: Arc<Provider<RetryClient<Http>>>,
        config: &MinerConfig,
        miner_id: H256,
    ) -> broadcast::Receiver<MineContextMessage> {
        let mine_contract = PoraMine::new(config.mine_address, provider.clone());
        let flow_contract = ZgsFlow::new(config.flow_address, provider.clone());

        let (mine_context_sender, mine_context_receiver) =
            broadcast::channel::<MineContextMessage>(4096);
        let watcher = MineContextWatcher {
            provider,
            flow_contract,
            mine_contract,
            mine_context_sender,
            msg_recv,
            last_report: None,
            query_interval: config.context_query_interval,
            miner_id,
        };
        executor.spawn(
            async move { Box::pin(watcher.start()).await },
            "mine_context_watcher",
        );
        mine_context_receiver
    }

    async fn start(mut self) {
        let mut mining_enabled = true;
        let mut channel_opened = true;

        let mut mining_throttle = sleep(Duration::from_secs(0));
        tokio::pin!(mining_throttle);

        loop {
            tokio::select! {
                biased;

                v = self.msg_recv.recv(), if channel_opened => {
                    match v {
                        Ok(MinerMessage::ToggleMining(enable)) => {
                            mining_enabled = enable;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            channel_opened = false;
                        }
                        _ => {}
                    }
                }

                () = &mut mining_throttle, if !mining_throttle.is_elapsed() => {
                }

                _ = async {}, if mining_enabled && mining_throttle.is_elapsed() => {
                    mining_throttle.as_mut().reset(Instant::now() + self.query_interval);
                    if let Err(err) = self.query_recent_context().await {
                        warn!(err);
                    }
                }
            }
        }
    }

    async fn query_recent_context(&mut self) -> Result<(), String> {
        let miner_id = self.miner_id.0;
        let WorkerContext {
            context,
            pora_target,
            subtask_digest,
            max_shards,
        } = self
            .mine_contract
            .compute_worker_context(miner_id)
            .call()
            .await
            .map_err(|e| format!("Failed to query mining context: {:?}", e))?;

        let report = if pora_target > U256::zero() && context.digest != EMPTY_HASH.0 {
            Some(PoraPuzzle::new(
                context,
                pora_target,
                max_shards,
                H256(subtask_digest),
            ))
        } else {
            None
        };

        if report == self.last_report {
            return Ok(());
        }

        debug!("Update pora puzzle: {:?}", report);

        self.mine_context_sender
            .send(report.clone())
            .map_err(|e| format!("Failed to send out the most recent mine context: {:?}", e))?;
        self.last_report = report;

        Ok(())
    }
}
