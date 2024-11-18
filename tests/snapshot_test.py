#!/usr/bin/env python3

import os
import shutil
from test_framework.test_framework import TestFramework
from utility.utils import wait_until

class SnapshotTask(TestFramework):
    def setup_params(self):
        self.num_nodes = 2

    def run_test(self):
        # Stop the last node to verify historical file sync

        # Submit and upload files on node 0
        data_root_1 = self.__upload_file__(0, 256 * 1024)
        data_root_2 = self.__upload_file__(0, 256 * 1024)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1)["finalized"])
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2)["finalized"])

        # Start the last node to verify historical file sync
        self.nodes[1].shutdown()
        shutil.rmtree(os.path.join(self.nodes[1].data_dir, 'data_db'))
        
        self.start_storage_node(1)
        self.nodes[1].wait_for_rpc_connection()
        
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_1)["finalized"])
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2) is not None)
        wait_until(lambda: self.nodes[1].zgs_get_file_info(data_root_2)["finalized"])

if __name__ == "__main__":
    SnapshotTask().main()
