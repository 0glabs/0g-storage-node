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

impl Default for SubmitConfig {
    fn default() -> Self {
        Self {
            initial_gas_price: None,
            max_gas_price: Some(U256::from(1_000_000_000_000u64)), // 1000 Gneuron max
            max_gas: Some(U256::from(600_000)), // Based on successful tx
            gas_increase_factor: Some(12),
            max_retries: Some(100),
            interval_secs: Some(2),
        }
    }
}

/// A simple function to detect if the retry is from a mempool or timeout error.
/// Right now, we rely on `submit_once` returning `SubmissionAction::Retry` for ANY error
/// that is "retryable," so we must parse the error string or have a separate reason in a real app.
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
            if msg.contains("invalid nonce") || msg.contains("invalid sequence") {
                // Nonce error should be retried
                info!("Nonce error detected: {:?}", msg);
                return SubmissionAction::Retry(format!("nonce: {}", msg));
            } else if msg.contains("reverted") {
                // Contract reverted - this is a permanent error
                info!("Contract call reverted: {:?}", msg);
                return SubmissionAction::Error(format!("Contract reverted: {}", msg));
            } else if is_mempool_or_timeout_error(msg.clone()) {
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
            if receipt.status.unwrap_or_default().is_zero() {
                // Transaction was mined but failed
                info!("Transaction reverted on-chain. Receipt: {:?}", receipt);
                return SubmissionAction::Error("Transaction reverted on-chain".to_string());
            }
            info!("Transaction mined successfully, receipt: {:?}", receipt);
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
        Err(e) => {
            if e.to_string().contains("reverted") {
                info!("Transaction reverted with error: {:?}", e);
                SubmissionAction::Error(format!("Contract reverted: {:?}", e))
            } else {
                SubmissionAction::Error(format!("Transaction unrecoverable: {:?}", e))
            }
        }
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
    
    // Set minimum gas price to 265 Gneuron based on successful transaction
    let min_gas_price = U256::from(265_000_000_000u64); // 265 Gneuron
    let mut gas_price = if let Some(gp) = config.initial_gas_price {
        std::cmp::max(gp, min_gas_price)
    } else {
        let current_gas = middleware
            .get_gas_price()
            .await
            .map_err(|e| format!("Failed to fetch gas price: {:?}", e))?;
        std::cmp::max(current_gas, min_gas_price)
    };

    // If no factor is set, default to 12 => 20% bump
    let factor_num = config.gas_increase_factor.unwrap_or(12);
    let factor_den = 10u64;

    // Counter for non-gas retries
    let mut non_gas_retries = 0;
    let max_retries = config.max_retries.unwrap_or(100);

    // Get the initial nonce
    let sender = middleware.default_sender()
        .ok_or_else(|| "No default sender configured".to_string())?;
    
    let mut current_nonce = middleware
        .get_transaction_count(sender, None)
        .await
        .map_err(|e| format!("Failed to get initial nonce: {:?}", e))?;

    info!("Starting transaction submission with nonce: {} and gas price: {} Gneuron", 
        current_nonce, gas_price / U256::from(1_000_000_000u64));

    loop {
        // Set gas price and nonce on the call
        call = call.gas_price(gas_price).nonce(current_nonce);

        info!("Attempting transaction with nonce: {}, gas price: {} Gneuron", 
            current_nonce, gas_price / U256::from(1_000_000_000u64));

        // Try to estimate gas to catch reverts before sending
        match call.estimate_gas().await {
            Ok(estimate) => {
                info!("Gas estimate for transaction: {} units (nonce: {}, gas price: {} Gneuron)", 
                    estimate, current_nonce, gas_price / U256::from(1_000_000_000u64));
                
                // Set gas limit to estimate + 10% buffer if no max_gas specified
                if config.max_gas.is_none() {
                    let gas_limit = estimate + (estimate / 10);
                    call = call.gas(gas_limit);
                    debug!("Setting gas limit to estimate + 10%: {}", gas_limit);
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("reverted") {
                    // Try to get more details about the revert
                    info!("Pre-flight check failed - Contract would revert:");
                    info!("  Nonce: {}", current_nonce);
                    info!("  Gas price: {} Gneuron", gas_price / U256::from(1_000_000_000u64));
                    info!("  Error: {:?}", err_msg);
                    
                    // Try to decode revert reason if available
                    if let Some(data) = err_msg.find("0x") {
                        let hex_data = &err_msg[data..];
                        info!("  Revert data: {}", hex_data);
                    }
                    
                    return Err(format!("Transaction would revert: {}", err_msg));
                }
                // If it's not a revert, continue with submission
                debug!("Gas estimation failed (continuing anyway): {:?}", e);
            }
        }

        match submit_once(call.clone()).await {
            SubmissionAction::Success(receipt) => {
                info!("Transaction successful with nonce: {}, gas price: {} Gneuron", 
                    current_nonce, gas_price / U256::from(1_000_000_000u64));
                return Ok(receipt);
            }
            SubmissionAction::Retry(error_str) => {
                if error_str.starts_with("nonce:") {
                    // For nonce errors, get fresh nonce and wait
                    info!("Nonce mismatch detected, fetching new nonce...");
                    match middleware.get_transaction_count(sender, None).await {
                        Ok(new_nonce) => {
                            if new_nonce > current_nonce {
                                info!("Updating nonce: {} -> {}", current_nonce, new_nonce);
                                current_nonce = new_nonce;
                            }
                        }
                        Err(e) => debug!("Failed to fetch new nonce: {:?}", e),
                    }
                    sleep(Duration::from_secs(10)).await;
                    continue;
                } else if is_mempool_or_timeout_error(error_str.clone()) {
                    // Keep same nonce but bump gas price for mempool/timeout errors
                    let new_price = increase_gas_price_u256(gas_price, factor_num, factor_den);
                    
                    // Ensure new price is at least 10% higher
                    let min_increase = gas_price + (gas_price / 10);
                    gas_price = std::cmp::max(new_price, min_increase);
                    
                    // Check if gas price exceeds max_gas_price
                    if let Some(max_price) = config.max_gas_price {
                        if gas_price > max_price {
                            return Err(format!("Gas price {} Gneuron exceeds maximum allowed {} Gneuron", 
                                gas_price / U256::from(1_000_000_000u64),
                                max_price / U256::from(1_000_000_000u64)));
                        }
                    }
                    
                    info!("Keeping nonce {}, increased gas price to {} Gneuron for next attempt", 
                        current_nonce, gas_price / U256::from(1_000_000_000u64));
                    
                    sleep(Duration::from_secs(3)).await;
                } else {
                    // For other retryable errors, increment retry counter and get fresh nonce
                    non_gas_retries += 1;
                    if non_gas_retries > max_retries {
                        return Err(format!("Exceeded non-gas retries: {}", max_retries));
                    }
                    
                    // Get fresh nonce for next attempt
                    match middleware.get_transaction_count(sender, None).await {
                        Ok(new_nonce) => {
                            if new_nonce > current_nonce {
                                info!("Updating nonce for retry #{}: {} -> {}", 
                                    non_gas_retries, current_nonce, new_nonce);
                                current_nonce = new_nonce;
                            }
                        }
                        Err(e) => debug!("Failed to fetch new nonce: {:?}", e),
                    }
                    
                    info!(
                        "Non-gas retry #{} (nonce: {}, gas price: {} Gneuron): {}",
                        non_gas_retries,
                        current_nonce,
                        gas_price / U256::from(1_000_000_000u64),
                        error_str
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
            SubmissionAction::Error(e) => {
                if e.contains("Contract reverted") {
                    // Add more context to revert errors
                    info!("Contract revert details:");
                    info!("  Nonce: {}", current_nonce);
                    info!("  Gas price: {} Gneuron", gas_price / U256::from(1_000_000_000u64));
                    info!("  Error: {}", e);
                    
                    // Try to decode revert reason if available
                    if let Some(data) = e.find("0x") {
                        let hex_data = &e[data..];
                        info!("  Revert data: {}", hex_data);
                    }
                    
                    return Err(format!("Smart contract rejected the transaction: {}", e));
                }
                return Err(e);
            }
        }

        sleep(Duration::from_secs(
            config.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS),
        ))
        .await;
    }
}
