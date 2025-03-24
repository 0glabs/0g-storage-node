#!/usr/bin/env python3

import os
import time

from config.node_config import ZGS_KEY_FILE, ZGS_NODEID
from test_framework.test_framework import TestFramework
from utility.utils import p2p_port


class NetworkDiscoveryTest(TestFramework):
    """
    This is to test whether community nodes could connect to each other via UDP discovery.
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
            }

    def run_test(self):
        timeout_secs = 10

        for iter in range(timeout_secs + 1):
            assert iter < timeout_secs, "Timeout to discover nodes for peer connection"
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

            if total_connected >= self.num_nodes * (self.num_nodes - 1):
                break

        self.log.info("====================================")
        self.log.info("All nodes connected to each other successfully")


if __name__ == "__main__":
    NetworkDiscoveryTest().main()
