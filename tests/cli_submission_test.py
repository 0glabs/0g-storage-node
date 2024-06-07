#!/usr/bin/env python3

import base64
import random
import tempfile

from config.node_config import GENESIS_ACCOUNT
from utility.submission import ENTRY_SIZE, bytes_to_entries
from utility.utils import (
    assert_equal,
    wait_until,
)
from test_framework.test_framework import TestFramework


class CliSubmissionTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 2

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
            256 * 1024,
            256 * 1025,
            256 * 2048,
            256 * 16385,
            256 * 1024 * 64,
        ]

        for i, v in enumerate(data_size):
            self.__test_upload_file_with_cli(
                v, i + 1, False if v >= 256 * 1024 * 64 else True
            )

    def __test_upload_file_with_cli(self, size, submission_index, rand_data=True):
        node_idx = random.randint(0, self.num_nodes - 1)
        self.log.info("node index: %d, file size: %d", node_idx, size)

        file_to_upload = tempfile.NamedTemporaryFile(dir=self.root_dir, delete=False)
        data = random.randbytes(size) if rand_data else b"\x10" * size

        file_to_upload.write(data)
        file_to_upload.close()

        root = self._upload_file_use_cli(
            self.blockchain_nodes[0].rpc_url,
            self.contract.address(),
            GENESIS_ACCOUNT.key,
            self.nodes[node_idx].rpc_url,
            file_to_upload,
        )

        self.log.info("root: %s", root)
        wait_until(lambda: self.contract.num_submissions() == submission_index)

        client = self.nodes[node_idx]
        wait_until(lambda: client.zgs_get_file_info(root) is not None)
        wait_until(lambda: client.zgs_get_file_info(root)["finalized"])

        num_of_entris = bytes_to_entries(size)
        if num_of_entris > 1:
            start_idx = random.randint(0, num_of_entris - 2)
            end_idx = min(
                random.randint(start_idx + 1, num_of_entris - 1), start_idx + ENTRY_SIZE
            )

            assert_equal(
                client.zgs_download_segment(root, start_idx, end_idx),
                base64.b64encode(
                    data[start_idx * ENTRY_SIZE : end_idx * ENTRY_SIZE]
                ).decode("utf-8"),
            )

        for i in range(0, self.num_nodes):
            if node_idx == i:
                continue

            self.log.info("wait node %d", i)
            wait_until(lambda: self.nodes[i].zgs_get_file_info(root) is not None)
            self.nodes[i].admin_start_sync_file(submission_index - 1)
            wait_until(
                lambda: self.nodes[i].sync_status_is_completed_or_unknown(
                    submission_index - 1
                )
            )

            wait_until(lambda: self.nodes[i].zgs_get_file_info(root)["finalized"])

            # start_idx = random.randint(0, num_of_entris - 1)
            # end_idx = min(
            #     random.randint(start_idx + 1, num_of_entris), start_idx + ENTRY_SIZE
            # )

            assert_equal(
                client.zgs_download_segment(root, 0, 1),
                self.nodes[i].zgs_download_segment(root, 0, 1),
            )


if __name__ == "__main__":
    CliSubmissionTest().main()
