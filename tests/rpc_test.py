#!/usr/bin/env python3

import tempfile

from config.node_config import GENESIS_ACCOUNT
from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data
from utility.utils import (
    assert_equal,
    wait_until,
)


class RpcTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 2

    def run_test(self):
        client1 = self.nodes[0]
        client2 = self.nodes[1]

        chunk_data = b"\x00" * 256
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: self.contract.num_submissions(1) == 1)
        assert_equal(self.contract.num_submissions(), self.contract.num_submissions(1))

        wait_until(lambda: client1.zgs_get_file_info(data_root) is not None)
        assert_equal(client1.zgs_get_file_info(data_root)["finalized"], False)

        wait_until(lambda: client2.zgs_get_file_info(data_root) is not None)
        assert_equal(client2.zgs_get_file_info(data_root)["finalized"], False)

        segment = submit_data(client1, chunk_data)
        self.log.info("segment: %s", segment)

        wait_until(lambda: client1.zgs_get_file_info(data_root)["finalized"])
        assert_equal(
            client1.zgs_download_segment(data_root, 0, 1), segment[0]["data"]
        )

        client2.admin_start_sync_file(0)
        wait_until(lambda: client2.sync_status_is_completed_or_unknown(0))

        wait_until(lambda: client2.zgs_get_file_info(data_root)["finalized"])
        assert_equal(
            client2.zgs_download_segment(data_root, 0, 1), segment[0]["data"]
        )

        self.__test_upload_file_with_cli(client1)

        client2.shutdown()
        wait_until(lambda: client1.zgs_get_status() == 0)

    def __test_upload_file_with_cli(self, client1):
        # Test with uploading files with cli
        n_files = 1
        for size in [2, 1023, 2051]:
            self.log.debug("file size: %d", size)
            file_to_upload = tempfile.NamedTemporaryFile(
                dir=self.root_dir, delete=False
            )
            file_to_upload.write(b"\x10" * 256 * size)
            file_to_upload.close()

            root = self._upload_file_use_cli(
                self.blockchain_nodes[0].rpc_url,
                self.contract.address(),
                GENESIS_ACCOUNT.key,
                self.nodes[0].rpc_url,
                file_to_upload,
            )

            n_files += 1
            wait_until(lambda: self.contract.num_submissions() == n_files)

            wait_until(lambda: client1.zgs_get_file_info(root) is not None)
            wait_until(lambda: client1.zgs_get_file_info(root)["finalized"])

            for i in range(1, self.num_nodes):
                self.log.info("wait node %d", i)
                wait_until(lambda: self.nodes[i].zgs_get_file_info(root) is not None)
                self.nodes[i].admin_start_sync_file(n_files - 1)
                wait_until(
                    lambda: self.nodes[i].sync_status_is_completed_or_unknown(
                        n_files - 1
                    )
                )

                wait_until(
                    lambda: self.nodes[i].zgs_get_file_info(root)["finalized"]
                )

                assert_equal(
                    client1.zgs_download_segment(root, 0, 2),
                    self.nodes[i].zgs_download_segment(root, 0, 2),
                )


if __name__ == "__main__":
    RpcTest().main()
