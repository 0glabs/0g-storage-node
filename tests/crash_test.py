#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data
from utility.utils import wait_until


class CrashTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4

    def run_test(self):
        # 1. all nodes are running
        chunk_data = b"\x01" * 256
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root) is not None)

        segment = submit_data(self.nodes[0], chunk_data)
        self.log.info("segment: %s", segment)

        for i in range(self.num_nodes):
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )
            self.nodes[i].admin_start_sync_file(0)
            self.log.info("wait for node: %s", i)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root)["finalized"]
            )

        # 2: first node runnging, other nodes killed
        self.log.info("kill node")
        # kill node to simulate node crash
        for i in range(1, self.num_nodes):
            self.nodes[i].stop(kill=True)

        chunk_data = b"\x02" * 256 * 1024
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 2)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root) is not None)

        segment = submit_data(self.nodes[0], chunk_data)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root)["finalized"])

        self.start_storage_node(1)
        self.nodes[1].wait_for_rpc_connection()
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root) is not None)
        self.nodes[1].admin_start_sync_file(1)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root)["finalized"])

        for i in range(2, self.num_nodes):
            self.start_storage_node(i)
            self.nodes[i].wait_for_rpc_connection()
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )
            self.nodes[i].admin_start_sync_file(1)

            self.nodes[i].stop(kill=True)
            self.start_storage_node(i)
            self.nodes[i].wait_for_rpc_connection()
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )
            self.nodes[i].admin_start_sync_file(1)
            
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root)["finalized"]
            )

        # 4: node[1..] synced contract entries and killed
        self.log.info("kill node 0")
        self.nodes[0].stop(kill=True)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()

        chunk_data = b"\x03" * 256
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 3)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root) is not None)

        for i in range(1, self.num_nodes):
            self.nodes[i].stop(kill=True)

        segment = submit_data(self.nodes[0], chunk_data)
        self.log.info("segment: %s", segment)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root)["finalized"])

        for i in range(1, self.num_nodes):
            self.log.info("wait for node: %s", i)
            self.start_storage_node(i)
            self.nodes[i].wait_for_rpc_connection()
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )
            self.nodes[i].admin_start_sync_file(2)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root)["finalized"]
            )

        # 5: node[1..] synced contract entries and killed, sync disorder
        self.nodes[0].stop(kill=True)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()

        chunk_data = b"\x04" * 256 * 1561
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 4)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root) is not None)

        for i in range(1, 2):
            self.nodes[i].stop(kill=True)

        chunk_data1 = b"\x05" * 256
        submissions1, data_root1 = create_submission(chunk_data1)
        self.contract.submit(submissions1)
        wait_until(lambda: self.contract.num_submissions() == 5)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root1) is not None)

        for i in range(2, self.num_nodes):
            self.nodes[i].stop(kill=True)

        segment = submit_data(self.nodes[0], chunk_data)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root)["finalized"])

        segment = submit_data(self.nodes[0], chunk_data1)
        wait_until(lambda: self.nodes[0].zgs_get_file_info(data_root1)["finalized"])

        for i in range(1, self.num_nodes):
            self.log.info("wait for node: %s", i)
            self.start_storage_node(i)
            self.nodes[i].wait_for_rpc_connection()
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root1) is not None
            )
            self.nodes[i].admin_start_sync_file(4)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root1)["finalized"]
            )

            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root) is not None
            )
            self.nodes[i].admin_start_sync_file(3)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root)["finalized"]
            )


if __name__ == "__main__":
    CrashTest().main()
