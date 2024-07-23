use super::api::RpcServer;
use crate::types::{LocationInfo, NetworkInfo, PeerInfo};
use crate::{error, Context};
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use network::{multiaddr::Protocol, Multiaddr};
use std::collections::HashMap;
use std::net::IpAddr;
use storage::config::all_shards_available;
use sync::{FileSyncInfo, SyncRequest, SyncResponse};
use task_executor::ShutdownReason;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn find_file(&self, tx_seq: u64) -> RpcResult<()> {
        info!("admin_findFile({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::FindFile { tx_seq })
            .await?;

        match response {
            SyncResponse::FindFile { err } => {
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

    async fn get_file_location(
        &self,
        tx_seq: u64,
        all_shards: bool,
    ) -> RpcResult<Option<Vec<LocationInfo>>> {
        info!("admin_getFileLocation()");

        let tx = match self.ctx.log_store.get_tx_by_seq_number(tx_seq).await? {
            Some(tx) => tx,
            None => {
                return Err(error::internal_error("tx not found"));
            }
        };
        let info: Vec<LocationInfo> = self
            .ctx
            .file_location_cache
            .get_all(tx.id())
            .iter()
            .map(|announcement| {
                let multiaddr: Multiaddr = announcement.at.clone().into();
                let found_ip: Option<IpAddr> =
                    multiaddr
                        .iter()
                        .fold(None, |found_ip, protocol| match protocol {
                            Protocol::Ip4(ip) => Some(ip.into()),
                            Protocol::Ip6(ip) => Some(ip.into()),
                            Protocol::Tcp(_port) => found_ip,
                            _ => found_ip,
                        });
                (
                    found_ip,
                    self.ctx
                        .file_location_cache
                        .get_peer_config(&announcement.peer_id.clone().into()),
                )
            })
            .filter(|(found_ip, shard_config)| shard_config.is_some() && found_ip.is_some())
            .map(|(found_ip, shard_config)| LocationInfo {
                ip: found_ip.unwrap(),
                shard_config: shard_config.unwrap(),
            })
            .collect();

        if !all_shards || all_shards_available(info.iter().map(|info| info.shard_config).collect())
        {
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }
}
