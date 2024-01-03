use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "miner")]
pub trait Rpc {
    #[method(name = "start")]
    async fn start(&self) -> RpcResult<bool>;

    #[method(name = "stop")]
    async fn stop(&self) -> RpcResult<bool>;

    #[method(name = "setStartPosition")]
    async fn set_start_position(&self, index: u64) -> RpcResult<bool>;
}
