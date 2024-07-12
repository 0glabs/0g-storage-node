use contract_interface::zgs_flow::MineContext;
use ethereum_types::{H256, U256};
use rand::{self, Rng};
use std::time;
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, Duration, Instant};

use storage::config::ShardConfig;
use zgs_spec::{SECTORS_PER_LOAD, SECTORS_PER_MAX_MINING_RANGE, SECTORS_PER_PRICING};

use super::metrics;
use crate::recall_range::RecallRange;
use crate::{
    pora::{AnswerWithoutProof, Miner},
    watcher::MineContextMessage,
    MinerConfig, MinerMessage, PoraLoader,
};

use std::sync::Arc;

pub struct PoraService {
    mine_context_receiver: mpsc::UnboundedReceiver<MineContextMessage>,
    mine_answer_sender: mpsc::UnboundedSender<AnswerWithoutProof>,
    msg_recv: broadcast::Receiver<MinerMessage>,
    loader: Arc<dyn PoraLoader>,

    puzzle: Option<PoraPuzzle>,
    mine_range: MineRangeConfig,
    miner_id: H256,

    cpu_percentage: u64,
    iter_batch: usize,
}

struct PoraPuzzle {
    context: MineContext,
    target_quality: U256,
}
#[derive(Clone, Debug, Default)]
pub struct MineRangeConfig {
    start_position: Option<u64>,
    end_position: Option<u64>,
    shard_config: ShardConfig,
}

impl MineRangeConfig {
    #[inline]
    fn to_valid_range(&self, context: &MineContext) -> Option<RecallRange> {
        let self_start_position = self.start_position?;
        let self_end_position = self.end_position?;

        if self_start_position >= self_end_position {
            return None;
        }
        let minable_length =
            (context.flow_length.as_u64() / SECTORS_PER_LOAD as u64) * SECTORS_PER_LOAD as u64;

        let mining_length = std::cmp::min(minable_length, SECTORS_PER_MAX_MINING_RANGE as u64);

        let start_position = std::cmp::min(self_start_position, minable_length - mining_length);
        let start_position =
            (start_position / SECTORS_PER_PRICING as u64) * SECTORS_PER_PRICING as u64;

        Some(RecallRange {
            start_position,
            mining_length,
            shard_mask: self.shard_config.miner_shard_mask(),
            shard_id: self.shard_config.miner_shard_id(),
        })
    }

    #[inline]
    pub(crate) fn is_covered(&self, recall_position: u64) -> Option<bool> {
        let self_start_position = self.start_position?;
        let self_end_position = self.end_position?;

        if self.start_position >= self.end_position {
            return Some(false);
        }
        Some(
            self_start_position <= recall_position + SECTORS_PER_LOAD as u64
                || self_end_position > recall_position,
        )
    }
}

impl PoraService {
    pub fn spawn(
        executor: TaskExecutor,
        msg_recv: broadcast::Receiver<MinerMessage>,
        mine_context_receiver: mpsc::UnboundedReceiver<MineContextMessage>,
        loader: Arc<dyn PoraLoader>,
        config: &MinerConfig,
        miner_id: H256,
    ) -> mpsc::UnboundedReceiver<AnswerWithoutProof> {
        let (mine_answer_sender, mine_answer_receiver) =
            mpsc::unbounded_channel::<AnswerWithoutProof>();
        let mine_range = MineRangeConfig {
            start_position: Some(0),
            end_position: Some(u64::MAX),
            shard_config: config.shard_config,
        };
        let pora = PoraService {
            mine_context_receiver,
            mine_answer_sender,
            msg_recv,
            puzzle: None,
            mine_range,
            miner_id,
            loader,
            cpu_percentage: config.cpu_percentage,
            iter_batch: config.iter_batch,
        };
        executor.spawn(async move { Box::pin(pora.start()).await }, "pora_master");
        mine_answer_receiver
    }

    async fn start(mut self) {
        let mut mining_enabled = true;
        let mut channel_opened = true;

        let cpu_percent: u64 = self.cpu_percentage;
        let diastole = sleep(Duration::from_secs(0));
        tokio::pin!(diastole);

        loop {
            tokio::select! {
                biased;

                v = self.msg_recv.recv(), if channel_opened => {
                    trace!("PoraService receives msg={:?}", v);
                    match v {
                        Ok(MinerMessage::ToggleMining(enable)) => {
                            info!("Toggle mining: {}", if enable { "on" } else { "off" });
                            mining_enabled = enable;
                        }
                        Ok(MinerMessage::SetStartPosition(pos)) => {
                            info!("Change start position to: {:?}", pos);
                            self.mine_range.start_position = pos;
                        }
                        Ok(MinerMessage::SetEndPosition(pos)) => {
                            info!("Change end position to: {:?}", pos);
                            self.mine_range.end_position = pos;
                        }
                        Ok(MinerMessage::SetShardConfig(shard_config)) => {
                            self.mine_range.shard_config = shard_config;
                        }
                        Err(broadcast::error::RecvError::Closed)=>{
                            warn!("Unexpected: Mine service config channel closed.");
                            channel_opened = false;
                        }
                        Err(_)=>{

                        }
                    }
                }

                maybe_msg = self.mine_context_receiver.recv() => {
                    if let Some(msg) = maybe_msg {
                        info!("Update mine service: {:?}", msg);
                        info!("Mine iterations statistics: {}", metrics::report());
                        self.puzzle = msg.map(|(context, target_quality)| PoraPuzzle {
                            context, target_quality
                        });
                    } else {
                        warn!("Mine context channel closed.");
                    }
                }

                () = &mut diastole, if !diastole.is_elapsed() => {
                }

                _ = async {}, if mining_enabled && cpu_percent > 0 && self.as_miner().map_or(false, |miner| miner.range.mining_length > 0) && diastole.is_elapsed() => {
                    let nonce = H256(rand::thread_rng().gen());
                    let miner = self.as_miner().unwrap();

                    let timer = time::Instant::now();

                    if let Some(answer) = miner.batch_iteration(nonce, self.iter_batch).await {
                        debug!("Hit Pora answer {:?}", answer);
                        if self.mine_answer_sender.send(answer).is_err() {
                            warn!("Mine submitter channel closed");
                        }
                    } else if cpu_percent < 100 {
                        // 2^64 ns = 500 years
                        let elapsed = timer.elapsed().as_nanos() as u64;
                        let diastole_time = elapsed / cpu_percent * (100 - cpu_percent);
                        diastole.as_mut().reset(Instant::now() + Duration::from_nanos(diastole_time));
                    }
                }
            }
        }
    }

    #[inline]
    fn as_miner(&self) -> Option<Miner> {
        let puzzle = self.puzzle.as_ref()?;

        let range = self.mine_range.to_valid_range(&puzzle.context)?;
        (range.mining_length > 0).then_some(())?;

        Some(Miner {
            range,
            miner_id: &self.miner_id,
            mine_range_config: &self.mine_range,
            context: &puzzle.context,
            target_quality: &puzzle.target_quality,
            loader: &*self.loader,
        })
    }
}
