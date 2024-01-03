#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data, data_to_segments
from utility.utils import wait_until


class ExampleTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x01" * 256 * 1025
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segments = data_to_segments(chunk_data)
        client.zgs_upload_segment(segments[1])
        segment = client.zgs_download_segment(data_root, 1024, 1025)
        print(segment)
        print(client.rpc.zgs_downloadSegmentWithProof([data_root, 1]))


if __name__ == "__main__":
    ExampleTest().main()
