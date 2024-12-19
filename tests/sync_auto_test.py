#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.utils import wait_until

class AutoSyncTest(TestFramework):
    def setup_params(self):
        self.num_nodes = 2

        # Enable auto sync
        for i in range(self.num_nodes):
            self.zgs_node_configs[i] = {
                "sync": {
                    "auto_sync_enabled": True
                }
            }

    def run_test(self):
        # Submit and upload files on node 0
        data_root_1 = self.__upload_file__(0, 256 * 1024)
        data_root_2 = self.__upload_file__(0, 256 * 1024)

        # Files should be available on node 1 via auto sync
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1)["finalized"])
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2)["finalized"])

if __name__ == "__main__":
    AutoSyncTest().main()
