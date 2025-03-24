#!/usr/bin/env python3

import os
import time

from config.node_config import ZGS_KEY_FILE, ZGS_NODEID
from test_framework.test_framework import TestFramework
from utility.utils import p2p_port


class NetworkTcpShardTest(TestFramework):
    """
    This is to test TCP connection for shard config mismatched peers of UDP discovery.
    """

    def setup_params(self):
        # 1 bootnode and 2 community nodes
        self.num_nodes = 3

        # setup for node 0 as bootnode
        self.zgs_node_key_files = [ZGS_KEY_FILE]
        bootnode_port = p2p_port(0)
        self.zgs_node_configs[0] = {
            # enable UDP discovery relevant configs
            "network_enr_address": "127.0.0.1",
            "network_enr_tcp_port": bootnode_port,
            "network_enr_udp_port": bootnode_port,
            # disable trusted nodes
            "network_libp2p_nodes": [],
            # custom shard config
            "shard_position": "0/4",
        }

        # setup node 1 & 2 as community nodes
        bootnodes = [f"/ip4/127.0.0.1/udp/{bootnode_port}/p2p/{ZGS_NODEID}"]
        for i in range(1, self.num_nodes):
            self.zgs_node_configs[i] = {
                # enable UDP discovery relevant configs
                "network_enr_address": "127.0.0.1",
                "network_enr_tcp_port": p2p_port(i),
                "network_enr_udp_port": p2p_port(i),
                # disable trusted nodes and enable bootnodes
                "network_libp2p_nodes": [],
                "network_boot_nodes": bootnodes,
                # custom shard config
                "shard_position": f"{i}/4",
            }

    def run_test(self):
        timeout_secs = 10

        for iter in range(timeout_secs):
            time.sleep(1)
            self.log.info("==================================== iter %s", iter)

            for i in range(self.num_nodes):
                info = self.nodes[i].rpc.admin_getNetworkInfo()
                self.log.info(
                    "Node[%s] peers: total = %s, banned = %s, disconnected = %s, connected = %s (in = %s, out = %s)",
                    i,
                    info["totalPeers"],
                    info["bannedPeers"],
                    info["disconnectedPeers"],
                    info["connectedPeers"],
                    info["connectedIncomingPeers"],
                    info["connectedOutgoingPeers"],
                )

                if i == timeout_secs - 1:
                    assert info["totalPeers"] == self.num_nodes - 1
                    assert info["bannedPeers"] == 0
                    assert info["disconnectedPeers"] == self.num_nodes - 1
                    assert info["connectedPeers"] == 0

        self.log.info("====================================")
        self.log.info("All nodes discovered but not connected for each other")


if __name__ == "__main__":
    NetworkTcpShardTest().main()
