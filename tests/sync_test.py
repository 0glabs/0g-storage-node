#!/usr/bin/env python3

import random

from test_framework.test_framework import TestFramework
from utility.submission import create_submission
from utility.submission import submit_data
from utility.utils import (
    assert_equal,
    wait_until,
)


class SyncTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 2

    def run_test(self):
        client1 = self.nodes[0]
        client2 = self.nodes[1]

        self.stop_storage_node(1)

        size = 256 * 1024
        chunk_data = random.randbytes(size)

        submissions, data_root = create_submission(chunk_data)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)

        wait_until(lambda: self.contract.num_submissions() == 1)

        wait_until(lambda: client1.zgs_get_file_info(data_root) is not None)
        assert_equal(client1.zgs_get_file_info(data_root)["finalized"], False)

        segments = submit_data(client1, chunk_data)
        self.log.info(
            "segments: %s", [(s["root"], s["index"], s["proof"]) for s in segments]
        )

        wait_until(lambda: client1.zgs_get_file_info(data_root)["finalized"])

        self.start_storage_node(1)
        self.nodes[1].wait_for_rpc_connection()

        client2.admin_start_sync_file(0)
        wait_until(lambda: client2.sycn_status_is_completed_or_unknown(0))

        wait_until(lambda: client2.zgs_get_file_info(data_root)["finalized"])
        assert_equal(
            client2.zgs_download_segment(data_root, 0, 1),
            client1.zgs_download_segment(data_root, 0, 1),
        )


if __name__ == "__main__":
    SyncTest().main()
