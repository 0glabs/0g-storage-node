#![allow(unused)]

use contract_interface::{zgs_flow::MineContext, PoraMine, ZgsFlow};
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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{ops::DerefMut, str::FromStr};

use crate::{config::MineServiceMiddleware, mine::PoraPuzzle, MinerConfig, MinerMessage};

pub type MineContextMessage = Option<PoraPuzzle>;

lazy_static! {
    pub static ref EMPTY_HASH: H256 =
        H256::from_str("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").unwrap();
}

pub struct MineContextWatcher {
    provider: Arc<MineServiceMiddleware>,
    flow_contract: ZgsFlow<MineServiceMiddleware>,
    mine_contract: PoraMine<MineServiceMiddleware>,

    mine_context_sender: broadcast::Sender<MineContextMessage>,
    last_report: MineContextMessage,
    query_interval: Duration,

    msg_recv: broadcast::Receiver<MinerMessage>,
}

impl MineContextWatcher {
    pub fn spawn(
        executor: TaskExecutor,
        msg_recv: broadcast::Receiver<MinerMessage>,
        provider: Arc<MineServiceMiddleware>,
        config: &MinerConfig,
    ) -> broadcast::Receiver<MineContextMessage> {
        let provider = provider;

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
        let context_call = self.flow_contract.make_context_with_result();
        let valid_call = self.mine_contract.can_submit();
        let quality_call = self.mine_contract.pora_target();
        let shards_call = self.mine_contract.max_shards();

        let (context, can_submit, quality, max_shards) = try_join!(
            context_call.call(),
            valid_call.call(),
            quality_call.call(),
            shards_call.call()
        )
        .map_err(|e| format!("Failed to query mining context: {:?}", e))?;
        let report = if can_submit && context.digest != EMPTY_HASH.0 {
            Some(PoraPuzzle::new(context, quality, max_shards))
        } else {
            None
        };

        if report == self.last_report {
            return Ok(());
        }

        self.mine_context_sender
            .send(report.clone())
            .map_err(|e| format!("Failed to send out the most recent mine context: {:?}", e))?;
        self.last_report = report;

        Ok(())
    }
}
