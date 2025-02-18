use ethers::{
    abi::Detokenize,
    contract::ContractCall,
    providers::{Middleware, ProviderError},
    types::{TransactionReceipt, U256},
};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tracing::{debug, info};

/// The result of a single submission attempt.
#[derive(Debug)]
pub enum SubmissionAction {
    Success(TransactionReceipt),
    /// Generic "retry" signal, but we still need to know if it's "mempool/timeout" or something else.
    /// We'll parse the error string or have a separate reason in a real app.
    Retry(String),
    Error(String),
}

/// Configuration for submission retries, gas price, etc.
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct SubmitConfig {
    /// If `Some`, use this gas price for the first attempt.
    /// If `None`, fetch the current network gas price.
    pub(crate) initial_gas_price: Option<U256>,
    /// If `Some`, clamp increased gas price to this limit.
    /// If `None`, do not bump gas for mempool/timeout errors.
    pub(crate) max_gas_price: Option<U256>,
    /// Gas limit of the transaction
    pub(crate) max_gas: Option<U256>,
    /// Factor by which to multiply the gas price on each mempool/timeout error.
    /// E.g. if factor=11 => a 10% bump  => newGas = (gas * factor) / 10
    pub(crate) gas_increase_factor: Option<u64>,
    /// The maximum number of gas bumps (for mempool/timeout). If `max_gas_price` is set,
    /// we typically rely on clamping. But you can still cap the number of bumps if you want.
    pub(crate) max_retries: Option<usize>,
    /// Seconds to wait between attempts.
    pub(crate) interval_secs: Option<u64>,
}

const DEFAULT_INTERVAL_SECS: u64 = 2;
const DEFAULT_MAX_RETRIES: usize = 5;

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            initial_gas_price: None,
            max_gas_price: None,
            max_gas: None,
            gas_increase_factor: Some(11), // implies 10% bump if we do (gas*11)/10
            max_retries: Some(DEFAULT_MAX_RETRIES),
            interval_secs: Some(DEFAULT_INTERVAL_SECS),
        }
    }
}

/// A simple function to detect if the retry is from a mempool or timeout error.
/// Right now, we rely on `submit_once` returning `SubmissionAction::Retry` for ANY error
/// that is "retryable," so we must parse the error string from `submit_once`, or
/// store that string. Another approach is to return an enum with a reason from `submit_once`.
fn is_mempool_or_timeout_error(error_str: String) -> bool {
    let lower = error_str.to_lowercase();
    lower.contains("mempool") || lower.contains("timeout")
}

/// A function that performs a single submission attempt:
/// - Sends the transaction
/// - Awaits the receipt with limited internal retries
/// - Returns a `SubmissionAction` indicating success, retry, or error.
pub async fn submit_once<M, T>(call: ContractCall<M, T>) -> SubmissionAction
where
    M: Middleware + 'static,
    T: Detokenize,
{
    let pending_tx = match call.send().await {
        Ok(tx) => tx,
        Err(e) => {
            let msg = e.to_string();
            if is_mempool_or_timeout_error(msg.clone()) {
                return SubmissionAction::Retry(format!("mempool/timeout: {:?}", e));
            }

            debug!("Error sending transaction: {:?}", msg);
            return SubmissionAction::Error(format!("Transaction failed: {}", msg));
        }
    };

    debug!("Signed tx hash: {:?}", pending_tx.tx_hash());

    let receipt_result = pending_tx.await;
    match receipt_result {
        Ok(Some(receipt)) => {
            info!("Transaction mined, receipt: {:?}", receipt);
            SubmissionAction::Success(receipt)
        }
        Ok(None) => {
            debug!("Transaction probably timed out; retrying");
            SubmissionAction::Retry("timeout, receipt is none".to_string())
        }
        Err(ProviderError::HTTPError(e)) => {
            debug!("HTTP error retrieving receipt: {:?}", e);
            SubmissionAction::Retry(format!("http error: {:?}", e))
        }
        Err(e) => SubmissionAction::Error(format!("Transaction unrecoverable: {:?}", e)),
    }
}

/// Increase gas price using integer arithmetic: (gp * factor_num) / factor_den
fn increase_gas_price_u256(gp: U256, factor_num: u64, factor_den: u64) -> U256 {
    let num = U256::from(factor_num);
    let den = U256::from(factor_den);
    gp.checked_mul(num).unwrap_or(U256::MAX) / den
}

/// A higher-level function that wraps `submit_once` in a gas-price–adjustment loop,
/// plus a global timeout, plus distinct behavior for mempool/timeout vs other errors.
pub async fn submit_with_retry<M, T>(
    mut call: ContractCall<M, T>,
    config: &SubmitConfig,
    middleware: Arc<M>,
) -> Result<TransactionReceipt, String>
where
    M: Middleware + 'static,
    T: Detokenize,
{
    if let Some(max_gas) = config.max_gas {
        call = call.gas(max_gas);
    }
    let mut gas_price = if let Some(gp) = config.initial_gas_price {
        gp
    } else {
        middleware
            .get_gas_price()
            .await
            .map_err(|e| format!("Failed to fetch gas price: {:?}", e))?
    };

    // If no factor is set, default to 11 => 10% bump
    let factor_num = config.gas_increase_factor.unwrap_or(11);
    let factor_den = 10u64;

    // Two counters: one for gas bumps, one for non-gas retries
    let mut non_gas_retries = 0;
    let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);

    loop {
        // Set gas price on the call
        call = call.gas_price(gas_price);

        match submit_once(call.clone()).await {
            SubmissionAction::Success(receipt) => {
                return Ok(receipt);
            }
            SubmissionAction::Retry(error_str) => {
                // We need to figure out if it's "mempool/timeout" or some other reason.
                // Right now, we don't have the error string from `submit_once` easily,
                // so let's assume we store it or we do a separate function that returns it.
                // For simplicity, let's do a hack: let's define a placeholder "error_str" and parse it.
                // In reality, you'd likely return `SubmissionAction::Retry(reason_str)` from `submit_once`.
                if is_mempool_or_timeout_error(error_str.clone()) {
                    // Mempool/timeout error
                    if let Some(max_gp) = config.max_gas_price {
                        if gas_price >= max_gp {
                            return Err(format!(
                                "Exceeded max gas price: {}, with error msg: {}",
                                max_gp, error_str
                            ));
                        }
                        // Bump the gas
                        let new_price = increase_gas_price_u256(gas_price, factor_num, factor_den);
                        gas_price = std::cmp::min(new_price, max_gp);
                        debug!("Bumping gas price to {}", gas_price);
                    } else {
                        // No maxGasPrice => we do NOT bump => fail
                        return Err(
                            "Mempool/timeout error, no maxGasPrice set => aborting".to_string()
                        );
                    }
                } else {
                    // Non-gas error => increment nonGasRetries
                    non_gas_retries += 1;
                    if non_gas_retries > max_retries {
                        return Err(format!("Exceeded non-gas retries: {}", max_retries));
                    }
                    debug!(
                        "Non-gas retry #{} (same gas price: {})",
                        non_gas_retries, gas_price
                    );
                }
            }
            SubmissionAction::Error(e) => {
                return Err(e);
            }
        }

        // Sleep between attempts
        sleep(Duration::from_secs(
            config.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
        ))
        .await;
    }
}
