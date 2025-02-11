use contract_interface::PoraAnswer;
use contract_interface::{PoraMine, ZgsFlow};
use ethereum_types::U256;
use ethers::abi::Detokenize;
use ethers::contract::ContractCall;
use ethers::prelude::{Http, Provider, RetryClient};
use ethers::providers::{Middleware, ProviderError};
use hex::ToHex;
use shared_types::FlowRangeProof;
use std::sync::Arc;
use std::time::Duration;
use storage::H256;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc};

use crate::config::{MineServiceMiddleware, MinerConfig};
use crate::pora::AnswerWithoutProof;
use crate::watcher::MineContextMessage;

use zgs_spec::{BYTES_PER_SEAL, SECTORS_PER_SEAL};

const SUBMISSION_RETRIES: usize = 15;
const ADJUST_GAS_RETRIES: usize = 20;

pub struct Submitter {
    mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
    mine_context_receiver: broadcast::Receiver<MineContextMessage>,
    mine_contract: PoraMine<MineServiceMiddleware>,
    flow_contract: ZgsFlow<Provider<RetryClient<Http>>>,
    default_gas_limit: Option<U256>,
    store: Arc<Store>,
    provider: Arc<Provider<RetryClient<Http>>>,
}

enum SubmissionAction {
    Retry,
    Success,
    Error(String),
}

impl Submitter {
    pub fn spawn(
        executor: TaskExecutor,
        mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
        mine_context_receiver: broadcast::Receiver<MineContextMessage>,
        provider: Arc<Provider<RetryClient<Http>>>,
        signing_provider: Arc<MineServiceMiddleware>,
        store: Arc<Store>,
        config: &MinerConfig,
    ) {
        let mine_contract = PoraMine::new(config.mine_address, signing_provider);
        let flow_contract = ZgsFlow::new(config.flow_address, provider.clone());
        let default_gas_limit = config.submission_gas;

        let submitter = Submitter {
            mine_answer_receiver,
            mine_context_receiver,
            mine_contract,
            flow_contract,
            store,
            default_gas_limit,
            provider,
        };
        executor.spawn(
            async move { Box::pin(submitter.start()).await },
            "mine_answer_submitter",
        );
    }

    async fn start(mut self) {
        let mut current_context_digest: Option<H256> = None;
        loop {
            tokio::select! {
                answer_msg = self.mine_answer_receiver.recv() => {
                    match answer_msg {
                        Some(answer) => {
                            if Some(answer.context_digest) != current_context_digest {
                                info!("Skip submission because of inconsistent context digest");
                                continue;
                            }
                            if let Err(e) = self.submit_answer(answer).await {
                                warn!(e);
                            }
                        }
                        None => {
                            warn!("Mine submitter stopped because mine answer channel is closed.");
                            return;
                        }
                    }
                }

                context_msg = self.mine_context_receiver.recv() => {
                    match context_msg {
                        Ok(puzzle) => {
                            current_context_digest = puzzle.map(|p| p.context_digest());
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Mine context channel closed.");
                        },
                        Err(_) => {}
                    }
                }
            }
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

        self.submit_with_retry(submission_call).await
    }

    async fn submit_with_retry<M: Middleware, T: Detokenize>(
        &self,
        mut submission_call: ContractCall<M, T>,
    ) -> Result<(), String> {
        let mut gas_price = self
            .provider
            .get_gas_price()
            .await
            .map_err(|e| format!("Failed to get current gas price {:?}", e))?;
        let mut n_retry = 0;
        while n_retry < ADJUST_GAS_RETRIES {
            n_retry += 1;
            submission_call = submission_call.gas_price(gas_price);
            match self.submit_once(submission_call.clone()).await {
                SubmissionAction::Retry => {
                    gas_price = next_gas_price(gas_price);
                }
                SubmissionAction::Success => {
                    return Ok(());
                }
                SubmissionAction::Error(e) => {
                    return Err(e);
                }
            }
        }

        Err("Submission failed after retries".to_string())
    }

    async fn submit_once<M: Middleware, T: Detokenize>(
        &self,
        submission_call: ContractCall<M, T>,
    ) -> SubmissionAction {
        let pending_transaction = match submission_call.send().await {
            Ok(tx) => tx,
            Err(e) => {
                if e.to_string().contains("insufficient funds")
                    || e.to_string().contains("out of gas")
                {
                    return SubmissionAction::Error(format!(
                        "Fail to execute PoRA submission transaction: {:?}",
                        e
                    ));
                }
                // Log the error and increase gas.
                debug!("Error sending transaction: {:?}", e);
                return SubmissionAction::Retry;
            }
        };

        debug!(
            "Signed submission transaction hash: {:?}",
            pending_transaction.tx_hash()
        );

        let receipt_result = pending_transaction
            .retries(SUBMISSION_RETRIES)
            .interval(Duration::from_secs(2))
            .await;

        match receipt_result {
            Ok(Some(receipt)) => {
                // Successfully executed the transaction.
                info!("Submit PoRA success, receipt: {:?}", receipt);
                SubmissionAction::Success
            }
            Ok(None) => {
                // The transaction did not complete within the specified waiting time.
                debug!(
                    "Transaction dropped after {} retries; increasing gas and retrying",
                    SUBMISSION_RETRIES
                );
                SubmissionAction::Retry
            }
            Err(ProviderError::HTTPError(e)) => {
                // For HTTP errors, increase gas and retry.
                debug!("HTTP error retrieving receipt: {:?}", e);
                SubmissionAction::Retry
            }
            Err(e) => {
                // For all other errors, return immediately.
                SubmissionAction::Error(format!(
                    "Fail to execute PoRA submission transaction: {:?}",
                    e
                ))
            }
        }
    }
}

// TODO: The conversion will be simpler if we optimize range proof structure.
fn flow_proof_to_pora_merkle_proof(flow_proof: FlowRangeProof) -> Vec<[u8; 32]> {
    let depth_in_sealed_data = SECTORS_PER_SEAL.trailing_zeros() as usize;
    let full_proof: Vec<[u8; 32]> = flow_proof.left_proof.lemma().iter().map(|h| h.0).collect();
    // Exclude `item`, the nodes in the sealed data subtree, and `root`.
    full_proof[depth_in_sealed_data + 1..full_proof.len() - 1].to_vec()
}

fn next_gas_price(current_gas_price: U256) -> U256 {
    current_gas_price * U256::from(11) / U256::from(10)
}
