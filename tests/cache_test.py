#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data, data_to_segments
from utility.utils import wait_until


class ExampleTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x01" * 256 * 1025
        submissions, data_root = create_submission(chunk_data)

        segments = data_to_segments(chunk_data)
        client.zgs_upload_segment(segments[0])
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)
        wait_until(lambda: not client.zgs_get_file_info(data_root)["isCached"] and client.zgs_get_file_info(data_root)["uploadedSegNum"] == 1)
        client.zgs_upload_segment(segments[1])
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    ExampleTest().main()
