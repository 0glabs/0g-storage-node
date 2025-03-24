#!/usr/bin/env python3

import os
import time

from config.node_config import ZGS_KEY_FILE, ZGS_NODEID
from test_framework.test_framework import TestFramework
from utility.utils import p2p_port


class NetworkDiscoveryUpgradeTest(TestFramework):
    """
    This is to test that low version community nodes could not connect to bootnodes.
    """

    def setup_params(self):
        # 1 bootnode and 1 community node
        self.num_nodes = 2

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
        }

        # setup node 1 as community node
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
                # disable network identity in ENR
                "discv5_disable_enr_network_id": True,
            }

    def run_test(self):
        for iter in range(10):
            time.sleep(1)
            self.log.info("==================================== iter %s", iter)

            total_connected = 0
            for i in range(self.num_nodes):
                info = self.nodes[i].rpc.admin_getNetworkInfo()
                total_connected += info["connectedPeers"]
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

            # ENR incompatible and should not discover each other for TCP connection
            assert total_connected == 0, "Nodes connected unexpectedly"

        self.log.info("====================================")
        self.log.info("ENR incompatible nodes do not connect to each other")


if __name__ == "__main__":
    NetworkDiscoveryUpgradeTest().main()
