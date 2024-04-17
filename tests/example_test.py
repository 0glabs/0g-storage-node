#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.submission import create_submission, submit_data
from utility.utils import wait_until


class ExampleTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x02" * 5253123
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        self.log.info("segment: %s", len(segment))
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    ExampleTest().main()
