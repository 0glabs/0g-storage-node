#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.utils import wait_until


class AutoSyncHistoricalTest(TestFramework):
    def setup_params(self):
        self.num_nodes = 4

        # Enable auto sync
        for i in range(self.num_nodes):
            self.zgs_node_configs[i] = {"sync": {"auto_sync_enabled": True}}

    def run_test(self):
        # Stop the last node to verify historical file sync
        self.stop_storage_node(self.num_nodes - 1)

        # Submit and upload files on node 0
        data_root_1 = self.__upload_file__(0, 256 * 1024)
        data_root_2 = self.__upload_file__(0, 256 * 1024)

        # Files should be available on other nodes via auto sync
        for i in range(1, self.num_nodes - 1):
            wait_until(lambda: self.nodes[i].zgs_get_file_info(data_root_1) is not None)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root_1)["finalized"]
            )
            wait_until(lambda: self.nodes[i].zgs_get_file_info(data_root_2) is not None)
            wait_until(
                lambda: self.nodes[i].zgs_get_file_info(data_root_2)["finalized"]
            )

        # Start the last node to verify historical file sync
        self.start_storage_node(self.num_nodes - 1)
        self.nodes[self.num_nodes - 1].wait_for_rpc_connection()
        wait_until(
            lambda: self.nodes[self.num_nodes - 1].zgs_get_file_info(data_root_1)
            is not None
        )
        wait_until(
            lambda: self.nodes[self.num_nodes - 1].zgs_get_file_info(data_root_1)[
                "finalized"
            ]
        )
        wait_until(
            lambda: self.nodes[self.num_nodes - 1].zgs_get_file_info(data_root_2)
            is not None
        )
        wait_until(
            lambda: self.nodes[self.num_nodes - 1].zgs_get_file_info(data_root_2)[
                "finalized"
            ]
        )


if __name__ == "__main__":
    AutoSyncHistoricalTest().main()
