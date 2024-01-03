#!/usr/bin/env python3

import random
import threading

from utility.submission import create_submission, submit_data
from utility.utils import (
    wait_until,
)
from test_framework.test_framework import TestFramework


class ParallelSubmissionTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 2

    def run_test(self):
        size = 245759
        thread_count = 16

        chunks = self.generate_data(size, thread_count)
        for submission, _, _ in chunks:
            self.log.info("submission: %s", submission)
            self.contract.submit(submission)

        wait_until(lambda: self.contract.num_submissions() == thread_count)

        client = self.nodes[0]
        for _, data_root, _ in chunks:
            wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        def submit(client, chunk_data):
            submit_data(client, chunk_data)

        threads = []
        for _, _, chunk_data in chunks:
            t = threading.Thread(target=submit, args=(client, chunk_data))
            threads.append(t)

        for t in threads[::-1]:
            t.start()

        for t in threads:
            t.join()

        for _, data_root, _ in chunks:
            wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

    def generate_data(self, size, num):
        res = []
        for _ in range(num):
            chunk_data = random.randbytes(size)
            submission = create_submission(chunk_data)
            res.append((submission[0], submission[1], chunk_data))

        return res


if __name__ == "__main__":
    ParallelSubmissionTest().main()
