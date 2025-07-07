#[macro_use]
extern crate tracing;

extern crate miner as zgs_miner;

mod admin;
mod config;
mod error;
mod middleware;
mod miner;
mod rpc_helper;
pub mod types;
mod zgs;
mod zgs_grpc;

use crate::miner::RpcServer as MinerRpcServer;
use crate::types::SegmentWithProof;
use crate::zgs_grpc::r#impl::ZgsGrpcServiceImpl;
use crate::zgs_grpc_proto::zgs_grpc_service_server::ZgsGrpcServiceServer;
use admin::RpcServer as AdminRpcServer;
use chunk_pool::MemoryChunkPool;
use file_location_cache::FileLocationCache;
use futures::channel::mpsc::Sender;
use jsonrpsee::core::RpcResult;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use network::{NetworkGlobals, NetworkMessage, NetworkSender};
use std::error::Error;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use storage_async::Store;
use sync::{SyncRequest, SyncResponse, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;
use zgs::RpcServer as ZgsRpcServer;
use zgs_miner::MinerMessage;

pub use admin::RpcClient as ZgsAdminRpcClient;
pub use config::Config as RPCConfig;
pub use miner::RpcClient as ZgsMinerRpcClient;
pub use zgs::RpcClient as ZgsRPCClient;

pub mod zgs_grpc_proto {
    tonic::include_proto!("zgs_grpc");
}

const DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/zgs_grpc_descriptor.bin");

/// A wrapper around all the items required to spawn the HTTP server.
///
/// The server will gracefully handle the case where any fields are `None`.
#[derive(Clone)]
pub struct Context {
    pub config: RPCConfig,
    pub file_location_cache: Arc<FileLocationCache>,
    pub network_globals: Arc<NetworkGlobals>,
    pub network_send: NetworkSender,
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
    let handles = if ctx.config.listen_address.port() != ctx.config.listen_address_admin.port() {
        run_server_public_private(ctx).await?
    } else {
        (run_server_all(ctx).await?, None)
    };

    info!("Rpc Server started");

    Ok(handles)
}

fn server_builder(ctx: Context) -> HttpServerBuilder<middleware::Metrics> {
    HttpServerBuilder::default()
        .max_request_body_size(ctx.config.max_request_body_size)
        .set_middleware(middleware::Metrics::default())
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
        .build(ctx.config.listen_address_admin)
        .await?
        .start(admin)?;

    Ok((handle_public, Some(handle_private)))
}

pub async fn run_grpc_server(ctx: Context) -> Result<(), Box<dyn Error>> {
    let grpc_addr = ctx.config.listen_address_grpc;
    let reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(DESCRIPTOR_SET)
        .build()?;

    let server = ZgsGrpcServiceServer::new(ZgsGrpcServiceImpl { ctx });
    Server::builder()
        .add_service(server)
        .add_service(reflection)
        .serve(grpc_addr)
        .await?;
    Ok(())
}

enum SegmentIndex {
    Single(usize),
    Range(usize, usize), // [start, end]
}

impl Debug for SegmentIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Single(val) => write!(f, "{}", val),
            Self::Range(start, end) => write!(f, "[{},{}]", start, end),
        }
    }
}

struct SegmentIndexArray {
    items: Vec<SegmentIndex>,
}

impl Debug for SegmentIndexArray {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self.items.first() {
            None => write!(f, "NULL"),
            Some(first) if self.items.len() == 1 => write!(f, "{:?}", first),
            _ => write!(f, "{:?}", self.items),
        }
    }
}

impl SegmentIndexArray {
    fn new(segments: &[SegmentWithProof]) -> Self {
        let mut items = Vec::new();

        let mut current = match segments.first() {
            None => return SegmentIndexArray { items },
            Some(seg) => SegmentIndex::Single(seg.index),
        };

        for index in segments.iter().skip(1).map(|seg| seg.index) {
            match current {
                SegmentIndex::Single(val) if val + 1 == index => {
                    current = SegmentIndex::Range(val, index)
                }
                SegmentIndex::Range(start, end) if end + 1 == index => {
                    current = SegmentIndex::Range(start, index)
                }
                _ => {
                    items.push(current);
                    current = SegmentIndex::Single(index);
                }
            }
        }

        items.push(current);

        SegmentIndexArray { items }
    }
}
