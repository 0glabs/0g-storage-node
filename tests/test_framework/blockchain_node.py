import os
import subprocess
import tempfile
import time
import rlp

from eth_utils import decode_hex, keccak
from web3 import Web3, HTTPProvider
from web3.middleware import construct_sign_and_send_raw_middleware
from enum import Enum, unique
from config.node_config import (
    GENESIS_PRIV_KEY,
    GENESIS_PRIV_KEY1,
    TX_PARAMS,
    MINER_ID,
)
from utility.simple_rpc_proxy import SimpleRpcProxy
from utility.utils import (
    initialize_config,
    wait_until,
    estimate_st_performance
)
from test_framework.contracts import load_contract_metadata


@unique
class BlockChainNodeType(Enum):
    Conflux = 0
    BSC = 1
    ZG = 2

    def block_time(self):
        if self == BlockChainNodeType.Conflux:
            return 0.5
        elif self == BlockChainNodeType.BSC:
            return 25 / estimate_st_performance()
        else:
            return 3.0

@unique
class NodeType(Enum):
    BlockChain = 0
    Zgs = 1


class FailedToStartError(Exception):
    """Raised when a node fails to start correctly."""


class TestNode:
    def __init__(
        self, node_type, index, data_dir, rpc_url, binary, config, log, rpc_timeout=10
    ):
        assert os.path.exists(binary), ("Binary not found: %s" % binary)
        self.node_type = node_type
        self.index = index
        self.data_dir = data_dir
        self.rpc_url = rpc_url
        self.config = config
        self.rpc_timeout = rpc_timeout
        self.process = None
        self.stdout = None
        self.stderr = None
        self.config_file = os.path.join(self.data_dir, "config.toml")
        self.args = [binary, "--config", self.config_file]
        self.running = False
        self.rpc_connected = False
        self.rpc = None
        self.log = log

    def __del__(self):
        if self.process:
            self.process.terminate()

    def __getattr__(self, name):
        """Dispatches any unrecognised messages to the RPC connection."""
        assert self.rpc_connected and self.rpc is not None, self._node_msg(
            "Error: no RPC connection"
        )
        return getattr(self.rpc, name)

    def _node_msg(self, msg: str) -> str:
        """Return a modified msg that identifies this node by its index as a debugging aid."""
        return "[node %s %d] %s" % (self.node_type, self.index, msg)

    def _raise_assertion_error(self, msg: str):
        """Raise an AssertionError with msg modified to identify this node."""
        raise AssertionError(self._node_msg(msg))

    def setup_config(self):
        os.mkdir(self.data_dir)
        initialize_config(self.config_file, self.config)

    def start(self, redirect_stderr=False):
        my_env = os.environ.copy()
        if self.stdout is None:
            self.stdout = tempfile.NamedTemporaryFile(
                dir=self.data_dir, prefix="stdout", delete=False
            )
        if self.stderr is None:
            self.stderr = tempfile.NamedTemporaryFile(
                dir=self.data_dir, prefix="stderr", delete=False
            )

        if redirect_stderr:
            self.process = subprocess.Popen(
                self.args,
                stdout=self.stdout,
                stderr=self.stdout,
                cwd=self.data_dir,
                env=my_env,
            )
        else:
            self.process = subprocess.Popen(
                self.args,
                stdout=self.stdout,
                stderr=self.stderr,
                cwd=self.data_dir,
                env=my_env,
            )
        self.running = True

    def wait_for_rpc_connection(self):
        raise NotImplementedError

    def _wait_for_rpc_connection(self, check):
        """Sets up an RPC connection to the node process. Returns False if unable to connect."""
        # Poll at a rate of four times per second
        poll_per_s = 4
        for _ in range(poll_per_s * self.rpc_timeout):
            if self.process.poll() is not None:
                raise FailedToStartError(
                    self._node_msg(
                        "exited with status {} during initialization".format(
                            self.process.returncode
                        )
                    )
                )
            rpc = SimpleRpcProxy(self.rpc_url, timeout=self.rpc_timeout)
            if check(rpc):
                self.rpc_connected = True
                self.rpc = rpc
                return
            time.sleep(1.0 / poll_per_s)
        self._raise_assertion_error(
            "failed to get RPC proxy: index = {}, rpc_url = {}".format(
                self.index, self.rpc_url
            )
        )

    def stop(self, expected_stderr="", kill=False, wait=True):
        """Stop the node."""
        if not self.running:
            return
        if kill:
            self.process.kill()
        else:
            self.process.terminate()
        if wait:
            self.wait_until_stopped(close_stdout_stderr=False)
        # Check that stderr is as expected
        self.stderr.seek(0)
        stderr = self.stderr.read().decode("utf-8").strip()
        # TODO: Check how to avoid `pthread lock: Invalid argument`.
        if stderr != expected_stderr and stderr != "pthread lock: Invalid argument":
            # print process status for debug
            if self.return_code is None:
                self.log.info("Process is still running")
            else:
                self.log.info(
                    "Process has terminated with code {}".format(self.return_code)
                )

            raise AssertionError(
                "Unexpected stderr {} != {} from node={}{}".format(
                    stderr, expected_stderr, self.node_type, self.index
                )
            )

        self.__safe_close_stdout_stderr__()

    def __safe_close_stdout_stderr__(self):
        if self.stdout is not None:
            self.stdout.close()
            self.stdout = None

        if self.stderr is not None:
            self.stderr.close()
            self.stderr = None

    def __is_node_stopped__(self):
        """Checks whether the node has stopped.

        Returns True if the node has stopped. False otherwise.
        This method is responsible for freeing resources (self.process)."""
        if not self.running:
            return True
        return_code = self.process.poll()
        if return_code is None:
            return False

        # process has stopped. Assert that it didn't return an error code.
        # assert return_code == 0, self._node_msg(
        #     "Node returned non-zero exit code (%d) when stopping" % return_code
        # )
        self.running = False
        self.process = None
        self.rpc = None
        self.log.debug("Node stopped")
        self.return_code = return_code
        return True

    def wait_until_stopped(self, close_stdout_stderr=True, timeout=20):
        wait_until(self.__is_node_stopped__, timeout=timeout)
        if close_stdout_stderr:
            self.__safe_close_stdout_stderr__()


class BlockchainNode(TestNode):
    def __init__(
        self,
        index,
        data_dir,
        rpc_url,
        binary,
        local_conf,
        contract_path,
        log,
        blockchain_node_type,
        rpc_timeout=10,
    ):
        self.contract_path = contract_path

        self.blockchain_node_type = blockchain_node_type

        super().__init__(
            NodeType.BlockChain,
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            log,
            rpc_timeout,
        )

    def wait_for_rpc_connection(self):
        self._wait_for_rpc_connection(lambda rpc: rpc.eth_syncing() is False)

    def wait_for_start_mining(self):
        self._wait_for_rpc_connection(lambda rpc: int(rpc.eth_blockNumber(), 16) > 0)

    def wait_for_transaction_receipt(self, w3, tx_hash, timeout=120, parent_hash=None):
        return w3.eth.wait_for_transaction_receipt(tx_hash, timeout)

    def setup_contract(self, enable_market, mine_period):
        w3 = Web3(HTTPProvider(self.rpc_url))

        account1 = w3.eth.account.from_key(GENESIS_PRIV_KEY)
        account2 = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        w3.middleware_onion.add(
            construct_sign_and_send_raw_middleware([account1, account2])
        )
        # account = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        # w3.middleware_onion.add(construct_sign_and_send_raw_middleware(account))

        def deploy_contract(name, args=None):
            if args is None:
                args = []
            contract_interface = load_contract_metadata(base_path=self.contract_path, name=name)
            contract = w3.eth.contract(
                abi=contract_interface["abi"],
                bytecode=contract_interface["bytecode"],
            )
            
            tx_params = TX_PARAMS.copy()
            del tx_params["gas"]
            tx_hash = contract.constructor(*args).transact(tx_params)
            tx_receipt = self.wait_for_transaction_receipt(w3, tx_hash)
            contract = w3.eth.contract(
                address=tx_receipt.contractAddress,
                abi=contract_interface["abi"],
            )
            return contract, tx_hash
        
        def predict_contract_address(offset):
            nonce = w3.eth.get_transaction_count(account1.address)
            rlp_encoded = rlp.encode([decode_hex(account1.address), nonce + offset])
            contract_address_bytes = keccak(rlp_encoded)[-20:]
            contract_address = '0x' + contract_address_bytes.hex()           
            return Web3.to_checksum_address(contract_address)
        
        def deploy_no_market():
            flowAddress = predict_contract_address(1)
            mineAddress = predict_contract_address(2)

            ZERO = "0x0000000000000000000000000000000000000000"

            self.log.debug("Start deploy contracts")
            book, _ = deploy_contract("AddressBook", [flowAddress, ZERO, ZERO, mineAddress]);
            self.log.debug("AddressBook deployed")

            flow_contract, flow_contract_hash = deploy_contract("Flow", [book.address, mine_period, 0])
            self.log.debug("Flow deployed")

            mine_contract, _ = deploy_contract("PoraMineTest", [book.address, 0])
            self.log.debug("Mine deployed")
            self.log.info("All contracts deployed")

            # tx_hash = mine_contract.functions.setMiner(decode_hex(MINER_ID)).transact(TX_PARAMS)
            # self.wait_for_transaction_receipt(w3, tx_hash)
            
            dummy_reward_contract = w3.eth.contract(
                address = book.functions.reward().call(),
                abi=load_contract_metadata(base_path=self.contract_path, name="IReward")["abi"],
            )

            return flow_contract, flow_contract_hash, mine_contract, dummy_reward_contract
        
        def deploy_with_market():
            mineAddress = predict_contract_address(1)
            marketAddress = predict_contract_address(2)
            rewardAddress = predict_contract_address(3)
            flowAddress = predict_contract_address(4)

            LIFETIME_MONTH = 1

            self.log.debug("Start deploy contracts")
            book, _ = deploy_contract("AddressBook", [flowAddress, marketAddress, rewardAddress, mineAddress]);
            self.log.debug("AddressBook deployed")

            mine_contract, _ = deploy_contract("PoraMineTest", [book.address, 0])
            deploy_contract("FixedPrice", [book.address, LIFETIME_MONTH])
            reward_contract, _ =deploy_contract("OnePoolReward", [book.address, LIFETIME_MONTH])
            flow_contract, flow_contract_hash = deploy_contract("FixedPriceFlow", [book.address, mine_period, 0])

            self.log.info("All contracts deployed")

            # tx_hash = mine_contract.functions.setMiner(decode_hex(MINER_ID)).transact(TX_PARAMS)
            # self.wait_for_transaction_receipt(w3, tx_hash)

            return flow_contract, flow_contract_hash, mine_contract, reward_contract
        
        if enable_market:
            return deploy_with_market()
        else:
            return deploy_no_market()

    def get_contract(self, contract_address):
        w3 = Web3(HTTPProvider(self.rpc_url))

        account1 = w3.eth.account.from_key(GENESIS_PRIV_KEY)
        account2 = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        w3.middleware_onion.add(
            construct_sign_and_send_raw_middleware([account1, account2])
        )

        contract_interface = load_contract_metadata(self.contract_path, "Flow")
        return w3.eth.contract(address=contract_address, abi=contract_interface["abi"])

    def wait_for_transaction(self, tx_hash):
        w3 = Web3(HTTPProvider(self.rpc_url))
        w3.eth.wait_for_transaction_receipt(tx_hash)

    def start(self):
        super().start(self.blockchain_node_type == BlockChainNodeType.BSC or self.blockchain_node_type == BlockChainNodeType.ZG)
