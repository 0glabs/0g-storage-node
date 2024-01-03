import os
import platform
import requests
import shutil
import stat

from config.node_config import BSC_CONFIG
from eth_utils import encode_hex
from utility.signature_utils import ec_random_keys, priv_to_addr
from test_framework.blockchain_node import BlockChainNodeType, BlockchainNode
from utility.utils import (
    blockchain_p2p_port,
    blockchain_rpc_port,
    is_windows_platform,
    wait_until,
)

__file_path__ = os.path.dirname(os.path.realpath(__file__))


class BSCNode(BlockchainNode):
    def __init__(
        self,
        index,
        root_dir,
        binary,
        updated_config,
        contract_path,
        token_contract_path,
        mine_contract_path,
        log,
        rpc_timeout=10,
    ):
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

        if not os.path.exists(self.binary):
            log.info("binary does not exist")
            dir_name = os.path.dirname(self.binary)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            try:
                with open(f"{self.binary}", "xb") as f:
                    self.__try_download_node(f, log)
            except FileExistsError:
                log.info("Binary is alrady under downloading")

        wait_until(lambda: os.access(f"{self.binary}", os.X_OK), timeout=120)

        self.node_id = encode_hex(priv_to_addr(ec_random_keys()[0]))

        super().__init__(
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            contract_path,
            token_contract_path,
            mine_contract_path,
            log,
            BlockChainNodeType.BSC,
            rpc_timeout,
        )

    def __try_download_node(self, f, log):
        url = "https://api.github.com/repos/{}/{}/releases/latest".format(
            "bnb-chain", "bsc"
        )
        req = requests.get(url)
        if req.ok:
            asset_name = self.__get_asset_name()

            url = ""
            for asset in req.json()["assets"]:
                if asset["name"].lower() == asset_name:
                    url = asset["browser_download_url"]
                    break

            if url:
                log.info("Try to download geth from %s", url)
                f.write(requests.get(url).content)
                f.close()

                if not is_windows_platform():
                    st = os.stat(self.binary)
                    os.chmod(self.binary, st.st_mode | stat.S_IEXEC)
        else:
            log.info("Request failed with %s", req)

    def __get_asset_name(self):
        sys = platform.system().lower()
        if sys == "linux":
            return "geth_linux"
        elif sys == "windows":
            return "geth_windows.exe"
        elif sys == "darwin":
            return "geth_mac"
        else:
            raise RuntimeError("Unable to recognize platform")

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
