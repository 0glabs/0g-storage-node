#!/usr/bin/env python3
import time

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data
from utility.utils import wait_until, assert_equal


class PrunerTest(TestFramework):

    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 16 * 1024,
            "prune_check_time_s": 1,
            "prune_batch_wait_time_ms": 10,
        }

    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x02" * 16 * 256 * 1024
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", len(segment))
        # Wait for 1 sec for the shard config to be updated
        time.sleep(1)
        shard_config = client.rpc.zgs_getShardConfig()
        shard_id = int(shard_config["shardId"])
        num_shard = int(shard_config["numShard"])

        for i in range(len(segment)):
            seg = client.zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            if i % num_shard == shard_id:
                # base64 encoding size
                assert_equal(len(seg), 349528)
            else:
                assert_equal(seg, None)


if __name__ == "__main__":
    PrunerTest().main()
