#!/usr/bin/env python3

import random

from test_framework.test_framework import TestFramework
from utility.submission import create_submission
from utility.submission import submit_data
from utility.utils import (
    assert_equal,
    wait_until,
)


class RandomTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4
        for i in range(self.num_nodes):
            self.zgs_node_configs[i] = {"find_peer_timeout_secs": 1, "confirmation_block_count": 1, "sync": {"auto_sync_enabled": True}}

    def run_test(self):
        max_size = 256 * 1024 * 64
        small_max_size = 256 * 64
        small_ratio = 0.9
        no_data_ratio = 0.2
        crash_ratio = 0.2
        clean_ratio = 0.3

        file_list = []
        # The number of files is set to a larger number for local tests.
        tx_count = 20
        for i in range(tx_count):
            chosen_node = random.randint(0, self.num_nodes - 1)
            if random.random() <= small_ratio:
                size = random.randint(0, small_max_size)
            else:
                size = random.randint(0, max_size)
            no_data = random.random() <= no_data_ratio
            self.log.info(f"choose {chosen_node}, seq={i}, size={size}, no_data={no_data}")

            client = self.nodes[chosen_node]
            chunk_data = random.randbytes(size)
            submissions, data_root = create_submission(chunk_data)
            self.contract.submit(submissions)
            wait_until(lambda: self.contract.num_submissions() == i + 1)
            wait_until(lambda: client.zgs_get_file_info(data_root) is not None, timeout=120)
            if not no_data:
                submit_data(client, chunk_data)
                wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])
                # Wait until the tx is sent out.
                for node_index in range(len(self.nodes)):
                    if node_index != chosen_node:
                        self.log.debug(f"check {node_index}")
                        wait_until(lambda: self.nodes[node_index].zgs_get_file_info(data_root) is not None, timeout=300)
                        wait_until(lambda: self.nodes[node_index].zgs_get_file_info(data_root)["finalized"], timeout=300)
            # TODO(zz): This is a temp solution to trigger auto sync after all nodes started.
            if i >= tx_count - 2:
                continue
            file_list.append((data_root, no_data))

            if random.random() <= crash_ratio:
                # TODO(zz): node 0 is the boot node.
                #  If it's crashed and cleaned, it will not be connected by others now.
                chosen_crash = random.randint(1, self.num_nodes - 1)
                clean = random.random() <= clean_ratio
                self.log.info(f"crash {chosen_crash}, clean={clean}")
                self.stop_storage_node(chosen_crash, clean)
                self.start_storage_node(chosen_crash)
                self.nodes[chosen_crash].wait_for_rpc_connection()

        for i in range(tx_count):
            for node in self.nodes:
                status = node.zgs_get_file_info_by_tx_seq(i)
                self.log.info(f"check tx seq {i}, {node.index}, status={status}")
        for data_root, no_data in file_list:
            if not no_data:
                for node in self.nodes:
                    self.log.debug(f"check {data_root}, {node.index}")
                    wait_until(lambda: node.zgs_get_file_info(data_root)["finalized"], timeout=300)


if __name__ == "__main__":
    RandomTest().main()
