#!/usr/bin/env python3
import time

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data, data_to_segments
from utility.utils import wait_until, assert_equal


class PrunerTest(TestFramework):

    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "0/2"
        }
        self.zgs_node_configs[1] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "1/2"
        }
        self.zgs_node_configs[3] = {
            "db_max_num_sectors": 2 ** 30,
            "shard_position": "1/4"
        }

    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x02" * 8 * 256 * 1024
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        # Submit data to two nodes with different shards.
        segments = data_to_segments(chunk_data)
        for i in range(len(segments)):
            client_index = i % 2
            self.nodes[client_index].zgs_upload_segment(segments[i])
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root)["finalized"])
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root)["finalized"])

        self.nodes[2].admin_start_sync_file(0)
        self.nodes[3].admin_start_sync_file(0)
        wait_until(lambda: self.nodes[2].sync_status_is_completed_or_unknown(0))
        wait_until(lambda: self.nodes[2].zgs_get_file_info(data_root)["finalized"])
        wait_until(lambda: self.nodes[3].sync_status_is_completed_or_unknown(0))
        wait_until(lambda: self.nodes[3].zgs_get_file_info(data_root)["finalized"])

        for i in range(len(segments)):
            index_store = i % 2
            index_empty = 1 - i % 2
            seg0 = self.nodes[index_store].zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            seg1 = self.nodes[index_empty].zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            seg2 = self.nodes[2].zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            seg3 = self.nodes[3].zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            # base64 encoding size
            assert_equal(len(seg0), 349528)
            assert_equal(seg1, None)
            # node 2 should save all data
            assert_equal(len(seg2), 349528)
            if i % 4 == 1:
                assert_equal(len(seg3), 349528)
            else:
                assert_equal(seg3, None)


if __name__ == "__main__":
    PrunerTest().main()
