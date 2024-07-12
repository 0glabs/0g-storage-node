use contract_interface::PoraAnswer;
use contract_interface::{PoraMine, ZgsFlow};
use ethereum_types::U256;
use ethers::contract::ContractCall;
use ethers::providers::PendingTransaction;
use hex::ToHex;
use shared_types::FlowRangeProof;
use std::sync::Arc;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::mpsc;

use crate::config::{MineServiceMiddleware, MinerConfig};
use crate::pora::AnswerWithoutProof;

use zgs_spec::{BYTES_PER_SEAL, SECTORS_PER_SEAL};

const SUBMISSION_RETIES: usize = 3;

pub struct Submitter {
    mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
    mine_contract: PoraMine<MineServiceMiddleware>,
    flow_contract: ZgsFlow<MineServiceMiddleware>,
    default_gas_limit: Option<U256>,
    store: Arc<Store>,
}

impl Submitter {
    pub fn spawn(
        executor: TaskExecutor,
        mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
        provider: Arc<MineServiceMiddleware>,
        store: Arc<Store>,
        config: &MinerConfig,
    ) {
        let mine_contract = PoraMine::new(config.mine_address, provider.clone());
        let flow_contract = ZgsFlow::new(config.flow_address, provider);
        let default_gas_limit = config.submission_gas;

        let submitter = Submitter {
            mine_answer_receiver,
            mine_contract,
            flow_contract,
            store,
            default_gas_limit,
        };
        executor.spawn(
            async move { Box::pin(submitter.start()).await },
            "mine_answer_submitter",
        );
    }

    async fn start(mut self) {
        loop {
            match self.mine_answer_receiver.recv().await {
                Some(answer) => {
                    if let Err(e) = self.submit_answer(answer).await {
                        warn!(e)
                    }
                }
                None => {
                    warn!("Mine submitter stopped because mine answer channel is closed.");
                    break;
                }
            };
        }
    }

    async fn submit_answer(&mut self, mine_answer: AnswerWithoutProof) -> Result<(), String> {
        debug!("submit answer: {:?}", mine_answer);
        let sealed_context_digest = self
            .flow_contract
            .query_context_at_position(
                (mine_answer.recall_position + SECTORS_PER_SEAL as u64 - 1) as u128,
            )
            .call()
            .await
            .map_err(|e| format!("Failed to fetch sealed contest digest: {:?}", e))?;
        debug!("Fetch sealed context: {:?}", sealed_context_digest);

        let flow_proof = self
            .store
            .get_proof_at_root(
                Some(mine_answer.context_flow_root),
                mine_answer.recall_position,
                SECTORS_PER_SEAL as u64,
            )
            .await
            .map_err(|e| e.to_string())?;

        let answer = PoraAnswer {
            context_digest: mine_answer.context_digest.0,
            nonce: mine_answer.nonce.0,
            miner_id: mine_answer.miner_id.0,
            range: mine_answer.range.into(),
            recall_position: mine_answer.recall_position.into(),
            seal_offset: mine_answer.seal_offset.into(),
            sealed_context_digest: sealed_context_digest.digest,
            sealed_data: unsafe {
                std::mem::transmute::<[u8; BYTES_PER_SEAL], [[u8; 32]; BYTES_PER_SEAL / 32]>(
                    mine_answer.sealed_data,
                )
            },
            merkle_proof: flow_proof_to_pora_merkle_proof(flow_proof),
        };
        trace!("submit_answer: answer={:?}", answer);

        let mut submission_call: ContractCall<_, _> = self.mine_contract.submit(answer).legacy();

        if let Some(gas_limit) = self.default_gas_limit {
            submission_call = submission_call.gas(gas_limit);
        }

        if let Some(calldata) = submission_call.calldata() {
            debug!(
                "Submission transaction calldata: {}",
                calldata.encode_hex::<String>()
            );
        }

        debug!("Local construct tx: {:?}", &submission_call.tx);
        debug!(
            "Estimate gas result: {:?}",
            submission_call.estimate_gas().await
        );

        let pending_transaction: PendingTransaction<'_, _> = submission_call
            .send()
            .await
            .map_err(|e| format!("Fail to send mine answer transaction: {:?}", e))?;

        debug!(
            "Signed submission transaction hash: {:?}",
            pending_transaction.tx_hash()
        );

        let receipt = pending_transaction
            .retries(SUBMISSION_RETIES)
            .await
            .map_err(|e| format!("Fail to execute mine answer transaction: {:?}", e))?
            .ok_or(format!(
                "Mine answer transaction dropped after {} retires",
                SUBMISSION_RETIES
            ))?;

        info!("Submit PoRA success");
        debug!("Receipt: {:?}", receipt);

        Ok(())
    }
}

// TODO: The conversion will be simpler if we optimize range proof structure.
fn flow_proof_to_pora_merkle_proof(flow_proof: FlowRangeProof) -> Vec<[u8; 32]> {
    let depth_in_sealed_data = SECTORS_PER_SEAL.trailing_zeros() as usize;
    let full_proof: Vec<[u8; 32]> = flow_proof.left_proof.lemma().iter().map(|h| h.0).collect();
    // Exclude `item`, the nodes in the sealed data subtree, and `root`.
    full_proof[depth_in_sealed_data + 1..full_proof.len() - 1].to_vec()
}
