#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data
from utility.utils import wait_until


class RecoveryTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x01" * 256 * 5
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", segment)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

        self.stop_storage_node(0)
        chunk_data = b"\x02" * 256 * 7
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 2)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", segment)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

        self.stop_storage_node(0)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

        # Test with larger data.
        chunk_data = b"\x03" * 256 * 1024 * 19
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 3)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
        self.stop_storage_node(0)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()
        submit_data(client, chunk_data)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])
        self.stop_storage_node(0)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    RecoveryTest().main()
