import os
import shutil
import base64

from config.node_config import ZGS_CONFIG
from test_framework.blockchain_node import NodeType, TestNode
from config.node_config import MINER_ID
from utility.utils import (
    initialize_toml_config,
    p2p_port,
    rpc_port,
    blockchain_rpc_port,
)


class ZgsNode(TestNode):
    def __init__(
        self,
        index,
        root_dir,
        binary,
        updated_config,
        log_contract_address,
        mine_contract_address,
        log,
        rpc_timeout=10,
        libp2p_nodes=None,
    ):
        local_conf = ZGS_CONFIG.copy()
        if libp2p_nodes is None:
            if index == 0:
                libp2p_nodes = []
            else:
                libp2p_nodes = []
                for i in range(index):
                    libp2p_nodes.append(f"/ip4/127.0.0.1/tcp/{p2p_port(i)}")

        indexed_config = {
            "network_libp2p_port": p2p_port(index),
            "network_discovery_port": p2p_port(index),
            "rpc_listen_address": f"127.0.0.1:{rpc_port(index)}",
            "rpc_listen_address_admin": "",
            "network_libp2p_nodes": libp2p_nodes,
            "log_contract_address": log_contract_address,
            "mine_contract_address": mine_contract_address,
            "blockchain_rpc_endpoint": f"http://127.0.0.1:{blockchain_rpc_port(0)}",
        }
        # Set configs for this specific node.
        local_conf.update(indexed_config)
        # Overwrite with personalized configs.
        local_conf.update(updated_config)
        data_dir = os.path.join(root_dir, "zgs_node" + str(index))
        rpc_url = "http://" + local_conf["rpc_listen_address"]
        super().__init__(
            NodeType.Zgs,
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            log,
            rpc_timeout,
        )

    def setup_config(self):
        os.mkdir(self.data_dir)
        log_config_path = os.path.join(self.data_dir, self.config["log_config_file"])
        with open(log_config_path, "w") as f:
            f.write("debug,hyper=info,h2=info")

        initialize_toml_config(self.config_file, self.config)

    def wait_for_rpc_connection(self):
        self._wait_for_rpc_connection(lambda rpc: rpc.zgs_getStatus() is not None)

    def start(self):
        self.log.info("Start zerog_storage node %d", self.index)
        super().start()

    # rpc
    def zgs_get_status(self):
        return self.rpc.zgs_getStatus()["connectedPeers"]

    def zgs_upload_segment(self, segment):
        return self.rpc.zgs_uploadSegment([segment])

    def zgs_download_segment(self, data_root, start_index, end_index):
        return self.rpc.zgs_downloadSegment([data_root, start_index, end_index])
    
    def zgs_download_segment_decoded(self, data_root: str, start_chunk_index: int, end_chunk_index: int) -> bytes:
        encodedSegment = self.rpc.zgs_downloadSegment([data_root, start_chunk_index, end_chunk_index])
        return None if encodedSegment is None else base64.b64decode(encodedSegment)

    def zgs_get_file_info(self, data_root):
        return self.rpc.zgs_getFileInfo([data_root])

    def zgs_get_file_info_by_tx_seq(self, tx_seq):
        return self.rpc.zgs_getFileInfoByTxSeq([tx_seq])

    def shutdown(self):
        self.rpc.admin_shutdown()
        self.wait_until_stopped()

    def admin_start_sync_file(self, tx_seq):
        return self.rpc.admin_startSyncFile([tx_seq])
    
    def admin_start_sync_chunks(self, tx_seq: int, start_chunk_index: int, end_chunk_index: int):
        return self.rpc.admin_startSyncChunks([tx_seq, start_chunk_index, end_chunk_index])

    def admin_get_sync_status(self, tx_seq):
        return self.rpc.admin_getSyncStatus([tx_seq])

    def sync_status_is_completed_or_unknown(self, tx_seq):
        status = self.rpc.admin_getSyncStatus([tx_seq])
        return status == "Completed" or status == "unknown"

    def clean_data(self):
        shutil.rmtree(os.path.join(self.data_dir, "db"))
