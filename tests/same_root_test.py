#!/usr/bin/env python3

import base64
import random
from test_framework.test_framework import TestFramework
from utility.submission import ENTRY_SIZE, submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)


class SubmissionTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def run_test(self):
        data_size = [
            2,
            256,
            256 * 1023 + 1,
            256 * 1024 * 256,
        ]
        same_root_tx_count = 2

        next_tx_seq = 0
        for i, v in enumerate(data_size):
            chunk_data = random.randbytes(v)
            # Send tx before uploading data
            for _ in range(same_root_tx_count):
                self.submit_tx_for_data(chunk_data, next_tx_seq)
                next_tx_seq += 1

            # Send tx and upload data.
            self.submit_tx_for_data(chunk_data, next_tx_seq)
            self.submit_data(chunk_data)
            next_tx_seq += 1
            # Check if all transactions are finalized
            for tx_offset in range(same_root_tx_count + 1):
                tx_seq = next_tx_seq - 1 - tx_offset
                # old txs are finalized after finalizing the new tx, so we may need to wait here.
                wait_until(lambda: self.nodes[0].zgs_get_file_info_by_tx_seq(tx_seq)["finalized"])

            # Send tx after uploading data
            for _ in range(same_root_tx_count):
                self.submit_tx_for_data(chunk_data, next_tx_seq, data_finalized=True)
                next_tx_seq += 1

    def submit_tx_for_data(self, chunk_data, tx_seq, data_finalized=False, node_idx=0):
        submissions, data_root = create_submission(chunk_data)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)

        wait_until(lambda: self.contract.num_submissions() == tx_seq + 1)

        client = self.nodes[node_idx]
        wait_until(lambda: client.zgs_get_file_info_by_tx_seq(tx_seq) is not None)
        wait_until(lambda: client.zgs_get_file_info_by_tx_seq(tx_seq)["finalized"] == data_finalized)

    def submit_data(self, chunk_data, node_idx=0):
        _, data_root = create_submission(chunk_data)
        client = self.nodes[node_idx]
        segments = submit_data(client, chunk_data)
        self.log.debug(
            "segments: %s", [(s["root"], s["index"], s["proof"]) for s in segments]
        )
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    SubmissionTest().main()
