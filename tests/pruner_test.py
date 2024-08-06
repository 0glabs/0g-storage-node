#!/usr/bin/env python3
import time

from test_framework.test_framework import TestFramework
from config.node_config import GENESIS_PRIV_KEY
from mine_with_market_test import PRICE_PER_SECTOR
from utility.submission import create_submission, submit_data
from utility.utils import wait_until, assert_equal, estimate_st_performance


class PrunerTest(TestFramework):

    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 16 * 1024,
            # "db_max_num_sectors": 32 * 1024 * 1024,
            "prune_check_time_s": 1,
            "prune_batch_wait_time_ms": 1,
        }
        self.enable_market = True
        self.mine_period = int(45 / self.block_time)
        self.lifetime_seconds = 240
        self.launch_wait_seconds = 15
        self.log.info("Contract Info: Est. block time %.2f, Mine period %d", self.block_time, self.mine_period)

    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x02" * 16 * 256 * 1024
        # chunk_data = b"\x02" * 5 * 1024 * 1024 * 1024
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions, tx_prarams = {"value": int(len(chunk_data) / 256 * PRICE_PER_SECTOR * 1.1)})
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", len(segment))
        # Wait for 1 sec for the shard config to be updated
        time.sleep(2)
        shard_config = client.rpc.zgs_getShardConfig()
        shard_id = int(shard_config["shardId"])
        num_shard = int(shard_config["numShard"])

        # wait_until(lambda: self.reward_contract.first_rewardable_chunk() != 0, timeout=180)
        # first_rewardable = self.reward_contract.first_rewardable_chunk() * 32 * 1024
        # Wait for 1 sec for the no reward segments to be pruned.
        time.sleep(1)
        # Wait for chunks to be removed.
        for i in range(len(segment)):
            seg = client.zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            # if i < first_rewardable or i % num_shard != shard_id:
            if i % num_shard != shard_id:
                assert_equal(seg, None)
            else:
                assert_equal(len(seg), 349528)


if __name__ == "__main__":
    PrunerTest().main()
