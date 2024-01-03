import os
import random
import threading
import time

from config.node_config import BLOCK_SIZE_LIMIT, CONFLUX_CONFIG
import eth_utils
from test_framework.blockchain_node import BlockChainNodeType, BlockchainNode
from utility.signature_utils import (
    encode_int32,
    get_nodeid,
    sha3,
)
from utility.simple_rpc_proxy import SimpleRpcProxy
from utility.utils import (
    blockchain_p2p_port,
    blockchain_rpc_port,
    blockchain_rpc_port_core,
    wait_until,
)
from web3.exceptions import TransactionNotFound


class ConfluxNode(BlockchainNode):
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
        local_conf = CONFLUX_CONFIG.copy()
        indexed_config = {
            "jsonrpc_http_eth_port": blockchain_rpc_port(index),
            "jsonrpc_local_http_port": blockchain_rpc_port_core(index),
            "tcp_port": blockchain_p2p_port(index),
        }
        # Set configs for this specific node.
        local_conf.update(indexed_config)
        # Overwrite with personalized configs.
        local_conf.update(updated_config)
        data_dir = os.path.join(root_dir, "blockchain_node" + str(index))
        rpc_url = (
            "http://"
            + local_conf["public_address"]
            + ":"
            + str(local_conf["jsonrpc_http_eth_port"])
        )
        self.ip = local_conf["public_address"]
        self.port = str(local_conf["tcp_port"])

        if "dev_block_interval_ms" in local_conf:
            self.auto_mining = True
        else:
            self.auto_mining = False

        # setup core space rpc
        core_space_rpc_url = (
            "http://"
            + local_conf["public_address"]
            + ":"
            + str(local_conf["jsonrpc_local_http_port"])
        )

        self.core_space_rpc = SimpleRpcProxy(core_space_rpc_url, timeout=rpc_timeout)

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
            BlockChainNodeType.Conflux,
            rpc_timeout,
        )

    def __getattr__(self, name):
        """Dispatches any unrecognised messages to the RPC connection."""
        assert self.rpc_connected and self.rpc is not None, self._node_msg(
            "Error: no RPC connection"
        )
        if name.startswith("eth_") or name.startswith("parity_"):
            return getattr(self.rpc, name)
        else:
            return getattr(self.core_space_rpc, name)

    def wait_for_transaction_receipt(self, w3, tx_hash, timeout=120, parent_hash=None):
        if self.auto_mining:
            return super().wait_for_transaction_receipt(w3, tx_hash, timeout)
        else:
            time_end = time.time() + timeout
            while time.time() < time_end:
                try:
                    tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
                except TransactionNotFound:
                    tx_receipt = None
                    if parent_hash:
                        parent_hash = self.generatefixedblock(
                            parent_hash, [], 1, False, None, None
                        )
                    else:
                        self.generateoneblock(1, BLOCK_SIZE_LIMIT)
                    time.sleep(0.5)

                if tx_receipt is not None:
                    return tx_receipt

            raise TransactionNotFound

    def wait_for_nodeid(self):
        pubkey, x, y = get_nodeid(self)
        self.key = eth_utils.encode_hex(pubkey)
        addr_tmp = bytearray(sha3(encode_int32(x) + encode_int32(y))[12:])
        addr_tmp[0] &= 0x0F
        addr_tmp[0] |= 0x10
        self.addr = addr_tmp
        self.log.debug("Get node {} nodeid {}".format(self.index, self.key))

    def best_block_hash(self):
        return self.core_space_rpc.cfx_getBestBlockHash()

    def cfx_epochNumber(self, epoch_number=None):
        return self.core_space_rpc.cfx_epochNumber([epoch_number])

    def getblockcount(self):
        return self.core_space_rpc.getblockcount()

    def addnode(self, key, peer_addr):
        return self.core_space_rpc.addnode([key, peer_addr])

    def removenode(self, key, peer_addr):
        return self.core_space_rpc.removenode([key, peer_addr])

    def addlatency(self, node_id, latency_ms):
        return self.core_space_rpc.addlatency([node_id, latency_ms])

    def getnodeid(self, challenge):
        return self.core_space_rpc.getnodeid([challenge])

    def generate_empty_blocks(self, num_blocks):
        return self.core_space_rpc.generate_empty_blocks([num_blocks])

    def generateoneblock(self, num_txs, block_size_limit):
        return self.core_space_rpc.generateoneblock([num_txs, block_size_limit])

    def generatefixedblock(
        self, parent_hash, referee, num_txs, adaptive, difficulty, pos_reference
    ):
        return self.core_space_rpc.generatefixedblock(
            [parent_hash, referee, num_txs, adaptive, difficulty, pos_reference]
        )


def check_handshake(from_connection, target_node_id):
    """
    Check whether node 'from_connection' has already
    added node 'target_node_id' into its peer set.
    """
    peers = from_connection.getpeerinfo()
    for peer in peers:
        if peer["nodeid"] == target_node_id and len(peer["protocols"]) > 0:
            return True
    return False


def get_peer_addr(connection):
    return "{}:{}".format(connection.ip, connection.port)


def connect_nodes(nodes, a, node_num, timeout=60):
    """
    Let node[a] connect to node[node_num]
    """
    from_connection = nodes[a]
    to_connection = nodes[node_num]
    key = nodes[node_num].key
    peer_addr = get_peer_addr(to_connection)
    from_connection.addnode(key, peer_addr)
    # poll until hello handshake complete to avoid race conditions
    # with transaction relaying
    wait_until(
        lambda: check_handshake(from_connection, to_connection.key), timeout=timeout
    )


def sync_blocks(rpc_connections, *, sync_count=True, wait=1, timeout=60):
    """
    Wait until everybody has the same tip.

    sync_blocks needs to be called with an rpc_connections set that has least
    one node already synced to the latest, stable tip, otherwise there's a
    chance it might return before all nodes are stably synced.
    """
    stop_time = time.time() + timeout
    while time.time() <= stop_time:
        best_hash = [x.best_block_hash() for x in rpc_connections]
        block_count = [x.getblockcount() for x in rpc_connections]
        if best_hash.count(best_hash[0]) == len(rpc_connections) and (
            not sync_count or block_count.count(block_count[0]) == len(rpc_connections)
        ):
            return
        time.sleep(wait)
    raise AssertionError(
        "Block sync timed out:{}".format(
            "".join("\n  {!r}".format(b) for b in best_hash + block_count)
        )
    )


def disconnect_nodes(nodes, from_connection, node_num):
    try:
        nodes[from_connection].removenode(
            nodes[node_num].key, get_peer_addr(nodes[node_num])
        )
        nodes[node_num].removenode(
            nodes[from_connection].key, get_peer_addr(nodes[from_connection])
        )
    except Exception as e:
        # If this node is disconnected between calculating the peer id
        # and issuing the disconnect, don't worry about it.
        # This avoids a race condition if we're mass-disconnecting peers.
        if e.error["code"] != -29:  # RPC_CLIENT_NODE_NOT_CONNECTED
            raise

    # wait to disconnect
    wait_until(
        lambda: [
            peer
            for peer in nodes[from_connection].getpeerinfo()
            if peer["nodeid"] == nodes[node_num].key
        ]
        == [],
        timeout=5,
    )
    wait_until(
        lambda: [
            peer
            for peer in nodes[node_num].getpeerinfo()
            if peer["nodeid"] == nodes[from_connection].key
        ]
        == [],
        timeout=5,
    )


def connect_sample_nodes(
    nodes, log, sample=3, latency_min=0, latency_max=300, timeout=30
):
    """
    Establish connections among nodes with each node having 'sample' outgoing peers.
    It first lets all the nodes link as a loop, then randomly pick 'sample-1'
    outgoing peers for each node.
    """
    peer = [[] for _ in nodes]
    latencies = [{} for _ in nodes]
    threads = []
    num_nodes = len(nodes)
    sample = min(num_nodes - 1, sample)

    for i in range(num_nodes):
        # make sure all nodes are reachable
        next = (i + 1) % num_nodes
        peer[i].append(next)
        lat = random.randint(latency_min, latency_max)
        latencies[i][next] = lat
        latencies[next][i] = lat

        for _ in range(sample - 1):
            while True:
                p = random.randint(0, num_nodes - 1)
                if p not in peer[i] and not p == i:
                    peer[i].append(p)
                    lat = random.randint(latency_min, latency_max)
                    latencies[i][p] = lat
                    latencies[p][i] = lat
                    break

    for i in range(num_nodes):
        t = ConnectThread(nodes, i, peer[i], latencies, log, min_peers=sample)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout)
        assert (
            not t.is_alive()
        ), "Node[{}] connect to other nodes timeout in {} seconds".format(t.a, timeout)
        assert not t.failed, "connect_sample_nodes failed."


class ConnectThread(threading.Thread):
    def __init__(self, nodes, a, peers, latencies, log, min_peers=3, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self.nodes = nodes
        self.a = a
        self.peers = peers
        self.latencies = latencies
        self.log = log
        self.min_peers = min_peers
        self.failed = False

    def run(self):
        try:
            while True:
                for i in range(len(self.peers)):
                    p = self.peers[i]
                    connect_nodes(self.nodes, self.a, p)
                for p in self.latencies[self.a]:
                    self.nodes[self.a].addlatency(
                        self.nodes[p].key, self.latencies[self.a][p]
                    )
                if len(self.nodes[self.a].getpeerinfo()) >= self.min_peers:
                    break
                else:
                    time.sleep(1)
        except Exception as e:
            node = self.nodes[self.a]
            self.log.error(
                "Node "
                + str(self.a)
                + " fails to be connected to "
                + str(self.peers)
                + ", ip={}, index={}".format(node.ip, node.index)
            )
            self.log.error(e)
            self.failed = True
