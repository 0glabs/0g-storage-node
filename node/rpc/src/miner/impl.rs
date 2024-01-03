use super::api::RpcServer;
use crate::Context;
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::{Error, RpcResult};
use miner::MinerMessage;
use tokio::sync::broadcast;

pub struct RpcServerImpl {
    pub ctx: Context,
}

impl RpcServerImpl {
    fn mine_service_sender(&self) -> &broadcast::Sender<MinerMessage> {
        self.ctx.mine_service_sender.as_ref().unwrap()
    }
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn start(&self) -> RpcResult<bool> {
        info!("mine_start()");
        let success = self
            .mine_service_sender()
            .send(MinerMessage::ToggleMining(true))
            .is_ok();
        Ok(success)
    }

    async fn stop(&self) -> RpcResult<bool> {
        info!("mine_stop()");

        let success = self
            .mine_service_sender()
            .send(MinerMessage::ToggleMining(false))
            .is_ok();
        Ok(success)
    }

    async fn set_start_position(&self, index: u64) -> RpcResult<bool> {
        info!("mine_setStartPosition({})", index);

        let success = self
            .mine_service_sender()
            .send(MinerMessage::SetStartPosition(Some(index)))
            .is_ok();
        Ok(success)
    }
}
