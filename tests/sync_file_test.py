#!/usr/bin/env python3

import time

from test_framework.test_framework import TestFramework
from utility.utils import assert_equal, wait_until

class SyncFileTest(TestFramework):
    """
    By default, auto_sync_enabled and sync_file_on_announcement_enabled are both false,
    and file sync should be triggered by rpc.
    """

    def setup_params(self):
        self.num_nodes = 2

    def run_test(self):
        client1 = self.nodes[0]
        client2 = self.nodes[1]

        # stop client2, preventing it from receiving AnnounceFile
        client2.shutdown()

        data_root = self.__upload_file__(0, 256 * 1024)

        # restart client2
        client2.start()
        client2.wait_for_rpc_connection()
        
        # File should not be auto sync on node 2 and there is no cached file locations
        wait_until(lambda: client2.zgs_get_file_info(data_root) is not None)
        time.sleep(3)
        assert_equal(client2.zgs_get_file_info(data_root)["finalized"], False)
        # file sync use ASK_FILE & ANSWER FILE protocol, and do not cache file announcement anymore.
        # assert(client2.admin_get_file_location(0) is None)

        # Trigger file sync by rpc
        assert(client2.admin_start_sync_file(0) is None)
        wait_until(lambda: client2.sync_status_is_completed_or_unknown(0))
        wait_until(lambda: client2.zgs_get_file_info(data_root)["finalized"])
        # file sync use ASK_FILE & ANSWER FILE protocol, and do not cache file announcement anymore.
        # assert(client2.admin_get_file_location(0) is not None)

        # Validate data
        assert_equal(
            client2.zgs_download_segment(data_root, 0, 1024),
            client1.zgs_download_segment(data_root, 0, 1024),
        )

if __name__ == "__main__":
    SyncFileTest().main()
