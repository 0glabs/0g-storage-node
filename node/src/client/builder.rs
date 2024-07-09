use super::{Client, RuntimeContext};
use chunk_pool::{ChunkPoolMessage, Config as ChunkPoolConfig, MemoryChunkPool};
use file_location_cache::FileLocationCache;
use log_entry_sync::{LogSyncConfig, LogSyncEvent, LogSyncManager};
use miner::{MineService, MinerConfig, MinerMessage};
use network::{
    self, Keypair, NetworkConfig, NetworkGlobals, NetworkMessage, RequestId,
    Service as LibP2PService,
};
use pruner::{Pruner, PrunerConfig, PrunerMessage};
use router::RouterService;
use rpc::RPCConfig;
use std::sync::Arc;
use storage::log_store::log_manager::LogConfig;
use storage::log_store::Store;
use storage::{LogManager, StorageConfig};
use sync::{SyncSender, SyncService};
use tokio::sync::{broadcast, mpsc, oneshot};

macro_rules! require {
    ($component:expr, $self:ident, $e:ident) => {
        $self
            .$e
            .as_ref()
            .ok_or(format!("{} requires {}", $component, std::stringify!($e)))?
    };
}

struct NetworkComponents {
    send: mpsc::UnboundedSender<NetworkMessage>,
    globals: Arc<NetworkGlobals>,
    keypair: Keypair,

    // note: these will be owned by the router service
    owned: Option<(
        LibP2PService<RequestId>,
        mpsc::UnboundedReceiver<NetworkMessage>,
    )>,
}

struct SyncComponents {
    send: SyncSender,
}

struct MinerComponents {
    send: broadcast::Sender<MinerMessage>,
}

struct LogSyncComponents {
    send: broadcast::Sender<LogSyncEvent>,
    catch_up_end_send: oneshot::Receiver<()>,
}

struct PrunerComponents {
    // note: these will be owned by the router service
    owned: Option<mpsc::UnboundedReceiver<PrunerMessage>>,
}

struct ChunkPoolComponents {
    send: mpsc::UnboundedSender<ChunkPoolMessage>,
}

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
#[derive(Default)]
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    store: Option<Arc<dyn Store>>,
    async_store: Option<Arc<storage_async::Store>>,
    file_location_cache: Option<Arc<FileLocationCache>>,
    network: Option<NetworkComponents>,
    sync: Option<SyncComponents>,
    miner: Option<MinerComponents>,
    log_sync: Option<LogSyncComponents>,
    pruner: Option<PrunerComponents>,
    chunk_pool: Option<ChunkPoolComponents>,
}

impl ClientBuilder {
    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn with_runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Initializes in-memory storage.
    pub fn with_memory_store(mut self) -> Result<Self, String> {
        // TODO(zz): Set config.
        let store = Arc::new(
            LogManager::memorydb(LogConfig::default())
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))?,
        );

        self.store = Some(store.clone());

        if let Some(ctx) = self.runtime_context.as_ref() {
            self.async_store = Some(Arc::new(storage_async::Store::new(
                store,
                ctx.executor.clone(),
            )));
        }

        Ok(self)
    }

    /// Initializes RocksDB storage.
    pub fn with_rocksdb_store(mut self, config: &StorageConfig) -> Result<Self, String> {
        let store = Arc::new(
            LogManager::rocksdb(LogConfig::default(), &config.db_dir)
                .map_err(|e| format!("Unable to start RocksDB store: {:?}", e))?,
        );

        self.store = Some(store.clone());

        if let Some(ctx) = self.runtime_context.as_ref() {
            self.async_store = Some(Arc::new(storage_async::Store::new(
                store,
                ctx.executor.clone(),
            )));
        }

        Ok(self)
    }

    pub fn with_file_location_cache(mut self) -> Self {
        let file_location_cache = Default::default();
        self.file_location_cache = Some(Arc::new(file_location_cache));
        self
    }

    /// Starts the networking stack.
    pub async fn with_network(mut self, config: &NetworkConfig) -> Result<Self, String> {
        let executor = require!("network", self, runtime_context).clone().executor;

        // construct the libp2p service context
        let service_context = network::Context { config };

        // construct communication channel
        let (send, recv) = mpsc::unbounded_channel::<NetworkMessage>();

        // launch libp2p service
        let (globals, keypair, libp2p) =
            LibP2PService::new(executor, send.clone(), service_context)
                .await
                .map_err(|e| format!("Failed to start network service: {:?}", e))?;

        self.network = Some(NetworkComponents {
            send,
            globals,
            keypair,
            owned: Some((libp2p, recv)),
        });

        Ok(self)
    }

    pub async fn with_sync(mut self, config: sync::Config) -> Result<Self, String> {
        let executor = require!("sync", self, runtime_context).clone().executor;
        let store = require!("sync", self, store).clone();
        let file_location_cache = require!("sync", self, file_location_cache).clone();
        let network_send = require!("sync", self, network).send.clone();
        let event_recv = require!("sync", self, log_sync).send.subscribe();

        let send = SyncService::spawn_with_config(
            config,
            executor,
            network_send,
            store,
            file_location_cache,
            event_recv,
        )
        .await
        .map_err(|e| format!("Failed to start sync service: {:?}", e))?;
        self.sync = Some(SyncComponents { send });

        Ok(self)
    }

    pub async fn with_miner(mut self, config: Option<MinerConfig>) -> Result<Self, String> {
        if let Some(config) = config {
            let executor = require!("miner", self, runtime_context).clone().executor;
            let network_send = require!("miner", self, network).send.clone();
            let store = self.async_store.as_ref().unwrap().clone();

            let send = MineService::spawn(executor, network_send, config, store).await?;
            self.miner = Some(MinerComponents { send });
        }

        Ok(self)
    }

    pub async fn with_pruner(mut self, config: Option<PrunerConfig>) -> Result<Self, String> {
        if let Some(config) = config {
            let miner_send = self.miner.as_ref().map(|miner| miner.send.clone());
            let store = require!("pruner", self, async_store).clone();
            let executor = require!("pruner", self, runtime_context).clone().executor;
            let recv = Pruner::spawn(executor, config, store, miner_send)
                .await
                .map_err(|e| e.to_string())?;
            self.pruner = Some(PrunerComponents { owned: Some(recv) });
        }
        Ok(self)
    }

    /// Starts the networking stack.
    pub fn with_router(mut self, router_config: router::Config) -> Result<Self, String> {
        let executor = require!("router", self, runtime_context).clone().executor;
        let sync_send = require!("router", self, sync).send.clone(); // note: we can make this optional in the future
        let miner_send = self.miner.as_ref().map(|x| x.send.clone());
        let chunk_pool_send = require!("router", self, chunk_pool).send.clone();
        let store = require!("router", self, store).clone();
        let file_location_cache = require!("router", self, file_location_cache).clone();

        let network = self.network.as_mut().ok_or("router requires a network")?;

        let (libp2p, network_recv) = network
            .owned
            .take() // router takes ownership of libp2p and network_recv
            .ok_or("router requires a network")?;
        let pruner_recv = self.pruner.as_mut().and_then(|pruner| pruner.owned.take());
        RouterService::spawn(
            executor,
            libp2p,
            network.globals.clone(),
            network_recv,
            network.send.clone(),
            sync_send,
            miner_send,
            chunk_pool_send,
            pruner_recv,
            store,
            file_location_cache,
            network.keypair.clone(),
            router_config,
        );

        Ok(self)
    }

    pub async fn with_rpc(
        mut self,
        rpc_config: RPCConfig,
        chunk_pool_config: ChunkPoolConfig,
    ) -> Result<Self, String> {
        if !rpc_config.enabled {
            return Ok(self);
        }

        let executor = require!("rpc", self, runtime_context).clone().executor;
        let async_store = require!("rpc", self, async_store).clone();
        let network_send = require!("rpc", self, network).send.clone();
        let mine_send = self.miner.as_ref().map(|x| x.send.clone());
        let synced_tx_recv = require!("rpc", self, log_sync).send.subscribe();

        let (chunk_pool, chunk_pool_handler) =
            chunk_pool::unbounded(chunk_pool_config, async_store.clone(), network_send.clone());
        let chunk_pool_components = ChunkPoolComponents {
            send: chunk_pool.sender(),
        };

        let chunk_pool_clone = chunk_pool.clone();
        let ctx = rpc::Context {
            config: rpc_config,
            network_globals: require!("rpc", self, network).globals.clone(),
            network_send,
            sync_send: require!("rpc", self, sync).send.clone(),
            log_store: async_store,
            chunk_pool,
            shutdown_sender: executor.shutdown_sender(),
            mine_service_sender: mine_send,
        };

        let (rpc_handle, maybe_admin_rpc_handle) = rpc::run_server(ctx.clone())
            .await
            .map_err(|e| format!("Unable to start HTTP RPC server: {:?}", e))?;

        executor.spawn(rpc_handle, "rpc");
        if let Some(admin_rpc_handle) = maybe_admin_rpc_handle {
            executor.spawn(admin_rpc_handle, "rpc_admin");
        }
        executor.spawn(chunk_pool_handler.run(), "chunk_pool_handler");
        executor.spawn(
            MemoryChunkPool::monitor_log_entry(chunk_pool_clone, synced_tx_recv),
            "chunk_pool_log_monitor",
        );

        self.chunk_pool = Some(chunk_pool_components);

        Ok(self)
    }

    pub async fn with_log_sync(mut self, config: LogSyncConfig) -> Result<Self, String> {
        let executor = require!("log_sync", self, runtime_context).clone().executor;
        let store = require!("log_sync", self, store).clone();
        let (send, catch_up_end_send) = LogSyncManager::spawn(config, executor, store)
            .await
            .map_err(|e| e.to_string())?;

        self.log_sync = Some(LogSyncComponents {
            send,
            catch_up_end_send,
        });
        Ok(self)
    }

    /// Consumes the builder, returning a `Client` if all necessary components have been
    /// specified.
    pub fn build(self) -> Result<Client, String> {
        require!("client", self, runtime_context);

        Ok(Client {
            network_globals: self.network.as_ref().map(|network| network.globals.clone()),
        })
    }
}
