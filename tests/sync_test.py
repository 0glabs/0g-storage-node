#!/usr/bin/env python3

import random
import time

from test_framework.test_framework import TestFramework
from utility.submission import data_to_segments
from utility.utils import (
    assert_equal,
    wait_until,
)

class SyncTest(TestFramework):
    def setup_params(self):
        self.num_nodes = 2

        # enable find chunks topic
        for i in range(self.num_nodes):
            self.zgs_node_configs[i] = {
                "network_find_chunks_enabled": True
            }

    def run_test(self):
        # By default, auto_sync_enabled and sync_file_on_announcement_enabled are both false,
        # and file or chunks sync should be triggered by rpc.
        self.__test_sync_file_by_rpc()
        self.__test_sync_chunks_by_rpc()

    def __test_sync_file_by_rpc(self):
        self.log.info("Begin to test file sync by rpc")

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

    def __test_sync_chunks_by_rpc(self):
        self.log.info("Begin to test chunks sync by rpc")

        client1 = self.nodes[0]
        client2 = self.nodes[1]

        # Prepare 3 segments to upload
        chunk_data = random.randbytes(256 * 1024 * 3)
        data_root = self.__submit_file__(chunk_data)

        # Ensure log entry sync from blockchain node
        wait_until(lambda: client1.zgs_get_file_info(data_root) is not None)
        assert_equal(client1.zgs_get_file_info(data_root)["finalized"], False)

        # Upload only 2nd segment to storage node
        segments = data_to_segments(chunk_data)
        self.log.info("segments: %s", [(s["root"], s["index"], s["proof"]) for s in segments])
        assert(client1.zgs_upload_segment(segments[1]) is None)

        # segment 0 is not able to download
        assert(client1.zgs_download_segment_decoded(data_root, 0, 1024) is None)
        # segment 1 is available to download
        assert_equal(client1.zgs_download_segment_decoded(data_root, 1024, 2048), chunk_data[1024*256:2048*256])
        # segment 2 is not able to download
        assert(client1.zgs_download_segment_decoded(data_root, 2048, 3072) is None)

        # Segment 1 should not be able to download on node 2
        wait_until(lambda: client2.zgs_get_file_info(data_root) is not None)
        assert_equal(client2.zgs_get_file_info(data_root)["finalized"], False)
        assert(client2.zgs_download_segment_decoded(data_root, 1024, 2048) is None)

        # Restart node 1 to check if the proof nodes are persisted.
        self.stop_storage_node(0)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()

        # Trigger chunks sync by rpc
        assert(client2.admin_start_sync_chunks(1, 1024, 2048) is None)
        wait_until(lambda: client2.sync_status_is_completed_or_unknown(1))
        wait_until(lambda: client2.zgs_download_segment_decoded(data_root, 1024, 2048) is not None)

        # Validate data
        assert_equal(client2.zgs_download_segment_decoded(data_root, 1024, 2048), chunk_data[1024*256:2048*256])

if __name__ == "__main__":
    SyncTest().main()
