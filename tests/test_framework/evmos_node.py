import os
import subprocess
import tempfile

from test_framework.blockchain_node import BlockChainNodeType, BlockchainNode
from utility.utils import blockchain_rpc_port, arrange_port

EVMOS_PORT_CATEGORY_WS = 0
EVMOS_PORT_CATEGORY_P2P = 1
EVMOS_PORT_CATEGORY_RPC = 2
EVMOS_PORT_CATEGORY_PPROF = 3

def evmos_init_genesis(root_dir: str, num_nodes: int):
    assert num_nodes > 0, "Invalid number of blockchain nodes: %s" % num_nodes

    shell_script = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), # test_framework folder
        "..", "config", "evmos-init-genesis.sh"
    )

    evmosd_dir = os.path.join(root_dir, "evmosd")
    os.mkdir(evmosd_dir)
    
    log_file = tempfile.NamedTemporaryFile(dir=evmosd_dir, delete=False, prefix="init_genesis_", suffix=".log")
    p2p_port_start = arrange_port(EVMOS_PORT_CATEGORY_P2P, 0)

    ret = subprocess.run(
        args=["bash", shell_script, evmosd_dir, str(num_nodes), str(p2p_port_start)],
        stdout=log_file,
        stderr=log_file,
    )

    log_file.close()

    assert ret.returncode == 0, "Failed to init evmos genesis, see more details in log file: %s" % log_file.name

class EvmosNode(BlockchainNode):
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
        assert len(updated_config) == 0, "updated_config not supported for evmos"

        data_dir = os.path.join(root_dir, "evmosd", "node" + str(index))
        rpc_url = "http://127.0.0.1:%s" % blockchain_rpc_port(index)

        super().__init__(
            index,
            data_dir,
            rpc_url,
            binary,
            {},
            contract_path,
            log,
            BlockChainNodeType.Evmos,
            rpc_timeout,
        )

        self.config_file = None
        self.args = [
            binary, "start",
            "--home", data_dir,
            # overwrite json rpc http port: 8545
            "--json-rpc.address", "127.0.0.1:%s" % blockchain_rpc_port(index),
            # overwrite json rpc ws port: 8546
            "--json-rpc.ws-address", "127.0.0.1:%s" % arrange_port(EVMOS_PORT_CATEGORY_WS, index),
            # overwrite p2p port: 26656
            "--p2p.laddr", "tcp://127.0.0.1:%s" % arrange_port(EVMOS_PORT_CATEGORY_P2P, index),
            # overwrite rpc port: 26657
            "--rpc.laddr", "tcp://127.0.0.1:%s" % arrange_port(EVMOS_PORT_CATEGORY_RPC, index),
            # overwrite pprof port: 6060
            "--rpc.pprof_laddr", "127.0.0.1:%s" % arrange_port(EVMOS_PORT_CATEGORY_PPROF, index),
        ]

    def setup_config(self):
        """ Already batch initialized by shell script in framework """
