#[macro_use]
extern crate tracing;

extern crate miner as zgs_miner;

mod admin;
mod config;
mod error;
mod miner;
pub mod types;
mod zgs;

use crate::miner::RpcServer as MinerRpcServer;
use admin::RpcServer as AdminRpcServer;
use chunk_pool::MemoryChunkPool;
use file_location_cache::FileLocationCache;
use futures::channel::mpsc::Sender;
use jsonrpsee::core::RpcResult;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use network::NetworkGlobals;
use network::NetworkMessage;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use storage_async::Store;
use sync::{SyncRequest, SyncResponse, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedSender;
use zgs::RpcServer as ZgsRpcServer;
use zgs_miner::MinerMessage;

pub use admin::RpcClient as ZgsAdminRpcClient;
pub use config::Config as RPCConfig;
pub use miner::RpcClient as ZgsMinerRpcClient;
pub use zgs::RpcClient as ZgsRPCClient;

/// A wrapper around all the items required to spawn the HTTP server.
///
/// The server will gracefully handle the case where any fields are `None`.
#[derive(Clone)]
pub struct Context {
    pub config: RPCConfig,
    pub file_location_cache: Arc<FileLocationCache>,
    pub network_globals: Arc<NetworkGlobals>,
    pub network_send: UnboundedSender<NetworkMessage>,
    pub sync_send: SyncSender,
    pub chunk_pool: Arc<MemoryChunkPool>,
    pub log_store: Arc<Store>,
    pub shutdown_sender: Sender<ShutdownReason>,
    pub mine_service_sender: Option<broadcast::Sender<MinerMessage>>,
}

impl Context {
    pub fn send_network(&self, msg: NetworkMessage) -> RpcResult<()> {
        self.network_send
            .send(msg)
            .map_err(|e| error::internal_error(format!("Failed to send network message: {:?}", e)))
    }

    pub async fn request_sync(&self, request: SyncRequest) -> RpcResult<SyncResponse> {
        self.sync_send
            .request(request)
            .await
            .map_err(|e| error::internal_error(format!("Failed to send sync request: {:?}", e)))
    }
}

pub async fn run_server(
    ctx: Context,
) -> Result<(HttpServerHandle, Option<HttpServerHandle>), Box<dyn Error>> {
    let handles = match ctx.config.listen_address_admin {
        Some(listen_addr_private) => run_server_public_private(ctx, listen_addr_private).await?,
        None => (run_server_all(ctx).await?, None),
    };

    info!("Server started");

    Ok(handles)
}

fn server_builder(ctx: Context) -> HttpServerBuilder {
    HttpServerBuilder::default().max_request_body_size(ctx.config.max_request_body_size)
}

/// Run a single RPC server for all namespace RPCs.
async fn run_server_all(ctx: Context) -> Result<HttpServerHandle, Box<dyn Error>> {
    // public rpc
    let mut zgs = (zgs::RpcServerImpl { ctx: ctx.clone() }).into_rpc();

    // admin rpc
    let admin = (admin::RpcServerImpl { ctx: ctx.clone() }).into_rpc();
    zgs.merge(admin)?;

    // mine rpc if configured
    if ctx.mine_service_sender.is_some() {
        let mine = (miner::RpcServerImpl { ctx: ctx.clone() }).into_rpc();
        zgs.merge(mine)?;
    }

    Ok(server_builder(ctx.clone())
        .build(ctx.config.listen_address)
        .await?
        .start(zgs)?)
}

/// Run 2 RPC servers (public & private) for different namespace RPCs.
async fn run_server_public_private(
    ctx: Context,
    listen_addr_private: SocketAddr,
) -> Result<(HttpServerHandle, Option<HttpServerHandle>), Box<dyn Error>> {
    // public rpc
    let zgs = (zgs::RpcServerImpl { ctx: ctx.clone() }).into_rpc();

    // admin rpc
    let mut admin = (admin::RpcServerImpl { ctx: ctx.clone() }).into_rpc();

    // mine rpc if configured
    if ctx.mine_service_sender.is_some() {
        let mine = (miner::RpcServerImpl { ctx: ctx.clone() }).into_rpc();
        admin.merge(mine)?;
    }

    let handle_public = server_builder(ctx.clone())
        .build(ctx.config.listen_address)
        .await?
        .start(zgs)?;

    let handle_private = server_builder(ctx.clone())
        .build(listen_addr_private)
        .await?
        .start(admin)?;

    Ok((handle_public, Some(handle_private)))
}
