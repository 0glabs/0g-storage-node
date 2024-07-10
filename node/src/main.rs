#[macro_use]
extern crate tracing;

mod cli;
mod client;
mod config;
mod log;

use crate::config::ZgsConfig;
use client::{Client, ClientBuilder, RuntimeContext};
use std::error::Error;

async fn start_node(context: RuntimeContext, config: ZgsConfig) -> Result<Client, String> {
    let network_config = config.network_config().await?;
    let storage_config = config.storage_config()?;
    let rpc_config = config.rpc_config()?;
    let log_sync_config = config.log_sync_config()?;
    let miner_config = config.mine_config()?;
    let router_config = config.router_config(&network_config)?;
    let pruner_config = config.pruner_config()?;

    ClientBuilder::default()
        .with_runtime_context(context)
        .with_rocksdb_store(&storage_config)?
        .with_log_sync(log_sync_config)
        .await?
        .with_file_location_cache()
        .with_network(&network_config)
        .await?
        .with_sync(config.sync)
        .await?
        .with_miner(miner_config)
        .await?
        .with_pruner(pruner_config)
        .await?
        .with_rpc(rpc_config, config.chunk_pool_config()?)
        .await?
        .with_router(router_config)?
        .build()
}

fn main() -> Result<(), Box<dyn Error>> {
    // Only allow 64-bit targets for compilation, since there are many
    // type conversions between `usize` and `u64`, or even use `usize`
    // as file size or chunk index.
    #[cfg(not(target_pointer_width = "64"))]
    compile_error!("compilation is only allowed for 64-bit targets");

    // enable backtraces
    std::env::set_var("RUST_BACKTRACE", "1");

    // runtime environment
    let mut environment = client::EnvironmentBuilder::new()
        .multi_threaded_tokio_runtime()?
        .build()?;

    let context = environment.core_context();
    let executor = context.executor.clone();

    // CLI, config, and logs
    let matches = cli::cli_app().get_matches();
    let config = ZgsConfig::parse(&matches)?;
    log::configure(
        &config.log_config_file,
        &config.log_directory,
        executor.clone(),
    );

    // start services
    executor.clone().spawn(
        async move {
            info!("Starting services...");
            if let Err(e) = start_node(context.clone(), config).await {
                error!(reason = %e, "Failed to start zgs node");
                // Ignore the error since it always occurs during normal operation when
                // shutting down.
                let _ =
                    executor
                        .shutdown_sender()
                        .try_send(task_executor::ShutdownReason::Failure(
                            "Failed to start zgs node",
                        ));
            } else {
                info!("Services started");
            }
        },
        "zgs_node",
    );

    // Block this thread until we get a ctrl-c or a task sends a shutdown signal.
    let shutdown_reason = environment.block_until_shutdown_requested()?;
    info!(reason = ?shutdown_reason, "Shutting down...");

    environment.fire_signal();

    // Shutdown the environment once all tasks have completed.
    environment.shutdown_on_idle();

    match shutdown_reason {
        task_executor::ShutdownReason::Success(_) => Ok(()),
        task_executor::ShutdownReason::Failure(msg) => Err(msg.to_string().into()),
    }
}
