#!/usr/bin/env python3
from test_framework.test_framework import TestFramework
from config.node_config import MINER_ID, GENESIS_PRIV_KEY
from utility.submission import create_submission, submit_data
from utility.utils import wait_until, assert_equal
from test_framework.blockchain_node import BlockChainNodeType


class RootConsistencyTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def submit_data(self, item, size):
        submissions_before = self.contract.num_submissions()
        client = self.nodes[0]
        chunk_data = item * 256 * size
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == submissions_before + 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

    def assert_flow_status(self, expected_length):
        contract_root = self.contract.get_flow_root().hex()
        contract_length = self.contract.get_flow_length()
        (node_root, node_length) = tuple(self.nodes[0].zgs_getFlowContext())

        assert_equal(contract_length, node_length)
        assert_equal(contract_length, expected_length)
        assert_equal(contract_root, node_root[2:])



    def run_test(self):
        self.assert_flow_status(1)

        self.submit_data(b"\x11", 1)
        self.assert_flow_status(2)

        self.submit_data(b"\x11", 8 + 4 + 2)
        self.assert_flow_status(16 + 4 + 2)

        self.submit_data(b"\x12", 128 + 64)
        self.assert_flow_status(256 + 64)

        self.submit_data(b"\x13", 512 + 256)
        self.assert_flow_status(1024 + 512 + 256)
        


if __name__ == "__main__":
    RootConsistencyTest().main()
