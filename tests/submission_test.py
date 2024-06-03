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
        self.num_blockchain_nodes = 2
        self.num_nodes = 3

    def run_test(self):
        data_size = [
            2,
            255,
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
            self.submission_data(v, i + 1, False if v >= 256 * 1024 * 256 else True)

    def submission_data(self, size, submission_index, rand_data=True):
        node_idx = random.randint(0, self.num_nodes - 1)
        self.log.info("node index: %d, file size: %d", node_idx, size)
        chunk_data = random.randbytes(size) if rand_data else b"\x10" * size

        submissions, data_root = create_submission(chunk_data)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)

        wait_until(lambda: self.contract.num_submissions() == submission_index)

        client = self.nodes[node_idx]
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
        assert_equal(client.zgs_get_file_info(data_root)["finalized"], False)

        segments = submit_data(client, chunk_data)
        self.log.info(
            "segments: %s", [(s["root"], s["index"], s["proof"]) for s in segments]
        )

        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

        first_entry = base64.b64decode(segments[0]["data"].encode("utf-8"))[
            0:ENTRY_SIZE
        ]

        assert_equal(
            base64.b64decode(
                client.zgs_download_segment(data_root, 0, 1).encode("utf-8")
            ),
            first_entry,
        )

        for i in range(0, self.num_nodes):
            if node_idx == i:
                continue

            # Wait for log entry before file sync, otherwise, admin_startSyncFile will be failed.
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )

            self.nodes[i].admin_start_sync_file(submission_index - 1)

            wait_until(
                lambda: self.nodes[i].sync_status_is_completed_or_unknown(
                    submission_index - 1
                )
            )

            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root)["finalized"]
            )

            assert_equal(
                base64.b64decode(
                    self.nodes[i]
                    .zgs_download_segment(data_root, 0, 1)
                    .encode("utf-8")
                ),
                first_entry,
            )


if __name__ == "__main__":
    SubmissionTest().main()
