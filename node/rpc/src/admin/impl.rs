use super::api::RpcServer;
use crate::types::{NetworkInfo, PeerInfo};
use crate::{error, Context};
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use std::collections::HashMap;
use sync::{FileSyncInfo, SyncRequest, SyncResponse};
use task_executor::ShutdownReason;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn shutdown(&self) -> RpcResult<()> {
        info!("admin_shutdown()");

        self.ctx
            .shutdown_sender
            .clone()
            .send(ShutdownReason::Success("Shutdown by admin"))
            .await
            .map_err(|e| error::internal_error(format!("Failed to send shutdown command: {:?}", e)))
    }

    #[tracing::instrument(skip(self), err)]
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()> {
        info!("admin_startSyncFile({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::SyncFile { tx_seq })
            .await?;

        match response {
            SyncResponse::SyncFile { err } => {
                if err.is_empty() {
                    Ok(())
                } else {
                    Err(error::internal_error(err))
                }
            }
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn start_sync_chunks(
        &self,
        tx_seq: u64,
        start_index: u64,
        end_index: u64,
    ) -> RpcResult<()> {
        info!("admin_startSyncChunks({tx_seq}, {start_index}, {end_index})");

        let response = self
            .ctx
            .request_sync(SyncRequest::SyncChunks {
                tx_seq,
                start_index,
                end_index,
            })
            .await?;

        match response {
            SyncResponse::SyncFile { err } => {
                if err.is_empty() {
                    Ok(())
                } else {
                    Err(error::internal_error(err))
                }
            }
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn terminate_sync(&self, tx_seq: u64) -> RpcResult<bool> {
        info!("admin_terminateSync({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::TerminateFileSync {
                tx_seq,
                is_reverted: false,
            })
            .await?;

        match response {
            SyncResponse::TerminateFileSync { count } => Ok(count > 0),
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn get_sync_status(&self, tx_seq: u64) -> RpcResult<String> {
        info!("admin_getSyncStatus({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::SyncStatus { tx_seq })
            .await?;

        match response {
            SyncResponse::SyncStatus { status } => Ok(status
                .map(|x| format!("{:?}", x))
                .unwrap_or_else(|| "unknown".into())),
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn get_sync_info(&self, tx_seq: Option<u64>) -> RpcResult<HashMap<u64, FileSyncInfo>> {
        info!(?tx_seq, "admin_getSyncInfo()");

        let response = self
            .ctx
            .request_sync(SyncRequest::FileSyncInfo { tx_seq })
            .await?;

        match response {
            SyncResponse::FileSyncInfo { result } => Ok(result),
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn get_network_info(&self) -> RpcResult<NetworkInfo> {
        info!("admin_getNetworkInfo()");

        let db = self.ctx.network_globals.peers.read();

        let connected_peers = db.connected_peers().count();
        let connected_outgoing_peers = db.connected_outbound_only_peers().count();

        Ok(NetworkInfo {
            peer_id: self.ctx.network_globals.local_peer_id().to_base58(),
            listen_addresses: self.ctx.network_globals.listen_multiaddrs(),
            total_peers: db.peers().count(),
            banned_peers: db.banned_peers().count(),
            disconnected_peers: db.disconnected_peers().count(),
            connected_peers,
            connected_outgoing_peers,
            connected_incoming_peers: connected_peers - connected_outgoing_peers,
        })
    }

    async fn get_peers(&self) -> RpcResult<HashMap<String, PeerInfo>> {
        info!("admin_getPeers()");

        Ok(self
            .ctx
            .network_globals
            .peers
            .read()
            .peers()
            .map(|(peer_id, info)| (peer_id.to_base58(), info.into()))
            .collect())
    }
}
