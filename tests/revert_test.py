#!/usr/bin/env python3

from test_framework.blockchain_node import BlockChainNodeType
from test_framework.test_framework import TestFramework
from test_framework.conflux_node import connect_nodes, disconnect_nodes, sync_blocks
from config.node_config import CONFLUX_CONFIG, TX_PARAMS1
from utility.submission import create_submission, submit_data
from utility.utils import wait_until


class RevertTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 1

        del CONFLUX_CONFIG["dev_block_interval_ms"]

    def run_test(self):
        blockchain_client1 = self.blockchain_nodes[0]
        blockchain_client2 = self.blockchain_nodes[1]

        self.log.info("Node 1 epoch {}".format(blockchain_client1.cfx_epochNumber()))
        self.log.info("Node 2 epoch {}".format(blockchain_client2.cfx_epochNumber()))

        disconnect_nodes(self.blockchain_nodes, 0, 1)
        blockchain_client1.generate_empty_blocks(5)

        self.log.info("Node 1 epoch {}".format(blockchain_client1.cfx_epochNumber()))
        self.log.info("Node 2 epoch {}".format(blockchain_client2.cfx_epochNumber()))

        client = self.nodes[0]
        chunk_data = b"\x00" * 256
        submissions, data_root = create_submission(chunk_data)

        tx_hash = self.contract.submit(submissions, 0)
        self.log.info("tx 1 hash: {}".format(tx_hash.hex()))
        wait_until(lambda: self.contract.num_submissions() == 1)
        assert client.zgs_get_file_info(data_root) is None
        # Generate blocks for confirmation
        blockchain_client1.generate_empty_blocks(12)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", segment)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

        self.log.info("Node 1 epoch {}".format(blockchain_client1.cfx_epochNumber()))
        self.log.info("Node 2 epoch {}".format(blockchain_client2.cfx_epochNumber()))

        self.log.info("===== submit tx to second node =====")
        chunk_data = b"\x10" * 256
        submissions, data_root1 = create_submission(chunk_data)
        tx_hash = self.contract.submit(submissions, 1, tx_prarams=TX_PARAMS1)
        self.log.info("tx 2 hash: {}".format(tx_hash.hex()))
        wait_until(lambda: self.contract.num_submissions(1) == 1)

        blockchain_client2.generate_empty_blocks(30)

        self.log.info("Node 1 epoch {}".format(blockchain_client1.cfx_epochNumber()))
        self.log.info("Node 2 epoch {}".format(blockchain_client2.cfx_epochNumber()))

        connect_nodes(self.blockchain_nodes, 0, 1)
        sync_blocks(self.blockchain_nodes[0:2])

        self.log.info("Node 1 epoch {}".format(blockchain_client1.cfx_epochNumber()))
        self.log.info("Node 2 epoch {}".format(blockchain_client2.cfx_epochNumber()))

        wait_until(lambda: client.zgs_get_file_info(data_root) is None)
        wait_until(lambda: client.zgs_get_file_info(data_root1) is not None)
        wait_until(lambda: not client.zgs_get_file_info(data_root1)["finalized"])

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", segment)
        wait_until(lambda: client.zgs_get_file_info(data_root1)["finalized"])


if __name__ == "__main__":
    RevertTest(blockchain_node_type=BlockChainNodeType.Conflux).main()
