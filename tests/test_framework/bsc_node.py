import os
import shutil

from config.node_config import BSC_CONFIG
from eth_utils import encode_hex
from utility.signature_utils import ec_random_keys, priv_to_addr
from test_framework.blockchain_node import BlockChainNodeType, BlockchainNode
from utility.utils import (
    blockchain_p2p_port,
    blockchain_rpc_port,
    wait_until,
)
from utility.build_binary import build_bsc

__file_path__ = os.path.dirname(os.path.realpath(__file__))


class BSCNode(BlockchainNode):
    def __init__(
        self,
        index,
        root_dir,
        binary,
        updated_config,
        contract_path,
        log,
        rpc_timeout=10,
    ):
        if not os.path.exists(binary):
            build_bsc(os.path.dirname(binary))

        local_conf = BSC_CONFIG.copy()
        indexed_config = {
            "HTTPPort": blockchain_rpc_port(index),
            "Port": blockchain_p2p_port(index),
        }
        # Set configs for this specific node.
        local_conf.update(indexed_config)
        # Overwrite with personalized configs.
        local_conf.update(updated_config)
        data_dir = os.path.join(root_dir, "blockchain_node" + str(index))
        rpc_url = "http://" + local_conf["HTTPHost"] + ":" + str(local_conf["HTTPPort"])

        self.genesis_config = os.path.join(
            __file_path__, "..", "config", "genesis.json"
        )
        self.binary = binary

        self.node_id = encode_hex(priv_to_addr(ec_random_keys()[0]))

        super().__init__(
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            contract_path,
            log,
            BlockChainNodeType.BSC,
            rpc_timeout,
        )

    def start(self):
        self.args = [
            self.binary,
            "--datadir",
            self.data_dir,
            "init",
            self.genesis_config,
        ]
        super().start()

        wait_until(lambda: self.process.poll() is not None)
        ret = self.process.poll()
        assert ret == 0, "BSC init should be successful"

        self.log.info("BSC node%d init finished with return code %d", self.index, ret)

        config_file = os.path.join(__file_path__, "..", "config", "bsc.toml")
        target = os.path.join(self.data_dir, "bsc.toml")
        shutil.copyfile(config_file, target)

        self.args = [
            self.binary,
            "--datadir",
            self.data_dir,
            "--port",
            str(self.config["Port"]),
            "--http",
            "--http.api",
            "personal,eth,net,web3,admin,txpool,miner",
            "--http.port",
            str(self.config["HTTPPort"]),
            # "--syncmode",
            # "full",
            # "--mine",
            "--miner.threads",
            "1",
            "--miner.etherbase",
            self.node_id,
            "--networkid",
            str(self.config["NetworkId"]),
            "--verbosity",
            str(self.config["Verbosity"]),
            "--config",
            "bsc.toml",
        ]

        self.log.info("Start BSC node %d", self.index)
        super().start()
