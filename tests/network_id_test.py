#!/usr/bin/env python3
import time

from test_framework.test_framework import TestFramework
from utility.utils import assert_equal


class NetworkIdTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 3
        self.zgs_node_configs[2] = {
            "blockchain_id": 1,
        }

    def run_test(self):
        time.sleep(2)
        # Node 0 and 1 will ban node 2 which has a different chain id.
        assert_equal(len(list(filter(is_banned, self.nodes[0].admin_getPeers().values()))), 1)
        assert_equal(len(list(filter(is_banned, self.nodes[1].admin_getPeers().values()))), 1)
        assert_equal(len(list(filter(is_banned, self.nodes[2].admin_getPeers().values()))), 2)


def is_banned(peer):
    return peer["connectionStatus"]["status"] == "banned"


if __name__ == "__main__":
    NetworkIdTest().main()
