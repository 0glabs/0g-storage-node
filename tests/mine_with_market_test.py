#!/usr/bin/env python3
from test_framework.test_framework import TestFramework
from config.node_config import MINER_ID, GENESIS_PRIV_KEY
from utility.submission import create_submission, submit_data
from utility.utils import wait_until, assert_equal, assert_greater_than, estimate_st_performance
from test_framework.blockchain_node import BlockChainNodeType
import time

import math

PRICE_PER_SECTOR = math.ceil(10 * (10 ** 18) / (2 ** 30) * 256 / 12)


class MineTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1
        self.zgs_node_configs[0] = {
            "db_max_num_sectors": 2**30,
            "miner_key": GENESIS_PRIV_KEY,
            "shard_position": "3 / 8",
        }
        self.enable_market = True
        self.mine_period = int(45 / self.block_time)
        self.launch_wait_seconds = 15
        self.log.info("Contract Info: Est. block time %.2f, Mine period %d", self.block_time, self.mine_period)

    def submit_data(self, item, size, no_submit = False):
        submissions_before = self.contract.num_submissions()
        client = self.nodes[0]
        chunk_data = item * 256 * size
        submissions, data_root = create_submission(chunk_data)
        value = int(size * PRICE_PER_SECTOR * 1.1)
        self.contract.submit(submissions, tx_prarams = {"value": value})
        wait_until(lambda: self.contract.num_submissions() == submissions_before + 1)
        
        if not no_submit:
            wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
            segment = submit_data(client, chunk_data)
            # wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

    def run_test(self):
        blockchain = self.blockchain_nodes[0]

        self.log.info("flow address: %s", self.contract.address())
        self.log.info("mine address: %s", self.mine_contract.address())

        difficulty = int(2**256 / 5 / estimate_st_performance())
        self.mine_contract.set_quality(difficulty)

        SECTORS_PER_PRICING = int(8 * ( 2 ** 30 ) / 256)


        self.log.info("Submit the actual data chunk (256 MB)")
        self.submit_data(b"\x11", int(SECTORS_PER_PRICING / 32))

        self.log.info("Submit the data hash only (8 GB)")
        self.submit_data(b"\x11", int(SECTORS_PER_PRICING), no_submit=True)
        # wait_until(lambda: self.contract.epoch() >= 1, timeout=180)

        start_epoch = self.contract.epoch()
        
        self.log.info("Submission Done, epoch is %d, current block number %d", start_epoch, int(blockchain.eth_blockNumber(), 16))  

        self.log.info("Wait for mine context release")
        wait_until(lambda: self.contract.epoch() >= start_epoch + 1, timeout=180)
        self.log.info("Current flow length: %d", self.contract.get_mine_context()[3])
        self.contract.update_context()

        self.log.info("Wait for mine answer")
        wait_until(lambda: self.mine_contract.last_mined_epoch() == start_epoch + 1 and not self.mine_contract.can_submit(), timeout=120)

        rewards = self.reward_contract.reward_distributes()
        assert_equal(len(rewards), 2)
        firstReward = rewards[0].args.amount
        self.log.info("Received reward %d Gwei", firstReward / (10**9))

        self.reward_contract.donate(10000 * 10 ** 18)
        self.log.info("Donation Done")
        self.log.info("Submit the data hash only (8 GB)")
        self.submit_data(b"\x11", int(SECTORS_PER_PRICING), no_submit=True)
        current_epoch = self.contract.epoch()
        assert_equal(current_epoch, start_epoch + 1);
        self.log.info("Sumission Done, epoch is %d, current block number %d", self.contract.epoch(), int(blockchain.eth_blockNumber(), 16))

        self.log.info("Wait for mine context release")
        wait_until(lambda: self.contract.epoch() >= start_epoch + 2, timeout=180)
        self.contract.update_context()

        self.log.info("Wait for mine answer")
        wait_until(lambda: self.mine_contract.last_mined_epoch() == start_epoch + 2 and not self.mine_contract.can_submit())
        assert_equal(self.contract.epoch(), start_epoch + 2)

        rewards = self.reward_contract.reward_distributes()
        assert_equal(len(rewards), 4)
        secondReward = rewards[2].args.amount
        self.log.info("Received reward %d Gwei", secondReward / (10**9))

        assert_greater_than(secondReward, 100 * firstReward / (start_epoch + 1))


if __name__ == "__main__":
    MineTest(blockchain_node_type=BlockChainNodeType.BSC).main()
