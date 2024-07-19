use crate::types::{LocationInfo, NetworkInfo, PeerInfo};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use std::collections::HashMap;
use sync::FileSyncInfo;

#[rpc(server, client, namespace = "admin")]
pub trait Rpc {
    #[method(name = "findFile")]
    async fn find_file(&self, tx_seq: u64) -> RpcResult<()>;

    #[method(name = "shutdown")]
    async fn shutdown(&self) -> RpcResult<()>;

    #[method(name = "startSyncFile")]
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()>;

    #[method(name = "startSyncChunks")]
    async fn start_sync_chunks(
        &self,
        tx_seq: u64,
        start_index: u64,
        end_index: u64, // exclusive
    ) -> RpcResult<()>;

    /// Terminate file or chunks sync for specified tx_seq.
    #[method(name = "terminateSync")]
    async fn terminate_sync(&self, tx_seq: u64) -> RpcResult<bool>;

    #[method(name = "getSyncStatus")]
    async fn get_sync_status(&self, tx_seq: u64) -> RpcResult<String>;

    #[method(name = "getSyncInfo")]
    async fn get_sync_info(&self, tx_seq: Option<u64>) -> RpcResult<HashMap<u64, FileSyncInfo>>;

    #[method(name = "getNetworkInfo")]
    async fn get_network_info(&self) -> RpcResult<NetworkInfo>;

    #[method(name = "getPeers")]
    async fn get_peers(&self) -> RpcResult<HashMap<String, PeerInfo>>;

    #[method(name = "getFileLocation")]
    async fn get_file_location(
        &self,
        tx_seq: u64,
        all_shards: bool,
    ) -> RpcResult<Option<Vec<LocationInfo>>>;
}
