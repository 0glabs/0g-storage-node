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
            "db_max_num_chunks": 16 * 1024,
            "miner_key": GENESIS_PRIV_KEY,
            "prune_check_time_s": 1,
            "prune_batch_wait_time_ms": 10,
        }
        self.enable_market = True
        self.mine_period = int(45 / self.block_time)
        self.lifetime_seconds = 60
        self.launch_wait_seconds = 15
        self.log.info("Contract Info: Est. block time %.2f, Mine period %d", self.block_time, self.mine_period)

    def run_test(self):
        difficulty = int(2**256 / 5 / estimate_st_performance())
        self.mine_contract.set_quality(difficulty)

        client = self.nodes[0]

        chunk_data = b"\x02" * 16 * 256 * 1024
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions, tx_prarams = {"value": int(len(chunk_data) / 256 * PRICE_PER_SECTOR * 1.1)})
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

        wait_until(lambda: self.reward_contract.first_rewardable_chunk() != 0)
        first_rewardable = self.reward_contract.first_rewardable_chunk()
        for i in range(shard_id, len(segment), num_shard):
            seg = client.zgs_download_segment(data_root, i * 1024, (i + 1) * 1024)
            if i < first_rewardable:
                assert_equal(seg, None)
            else:
                assert_equal(len(seg), 349528)


if __name__ == "__main__":
    PrunerTest().main()
