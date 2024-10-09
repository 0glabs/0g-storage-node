#!/usr/bin/env python3
import time

import base64
import random
from test_framework.test_framework import TestFramework
from utility.submission import ENTRY_SIZE, submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)


class ShardSubmitTest(TestFramework):

    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "0/4"
        }
        self.zgs_node_configs[1] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "1/4"
        }
        self.zgs_node_configs[2] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "2/4"
        }
        self.zgs_node_configs[3] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "3/4"
        }
    
    def run_test(self):
        data_size = [
            256*960, 
            256*1024,
            2,
            255,
            256*960,
            256*120,
            256,
            257,
            1023,
            1024,
            1025,
            256 * 1023,
            256 * 1023 + 1,
            256 * 1024,
            256 * 1024 + 1,
            256 * 1025,
            256 * 2048 - 1,
            256 * 2048,
            256 * 16385,
            256 * 1024 * 256,
        ]

        for i, v in enumerate(data_size):
            self.submission_data(v, i + 1, True)

    def submission_data(self, size, submission_index, rand_data=True):
        self.log.info("file size: %d", size)
        chunk_data = random.randbytes(size) if rand_data else b"\x10" * size

        submissions, data_root = create_submission(chunk_data)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)

        wait_until(lambda: self.contract.num_submissions() == submission_index)

        for i in range(4):
            client = self.nodes[i]
            wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
            submit_data(client, chunk_data)
            wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

if __name__ == "__main__":
    ShardSubmitTest().main()
