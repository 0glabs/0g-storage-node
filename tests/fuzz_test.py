#!/usr/bin/env python3

import random
import threading

from test_framework.test_framework import TestFramework
from config.node_config import TX_PARAMS, TX_PARAMS1
from utility.submission import create_submission, submit_data
from utility.utils import wait_until

SAMLL_SIZE = 350
LARGE_SIZE = 200
RADMON_SIZE = 300


class FuzzTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 4

    def run_test(self):
        lock = threading.Lock()
        nodes_index = [i for i in range(self.num_nodes)]

        account_lock = threading.Lock()
        accounts = [TX_PARAMS, TX_PARAMS1]

        def run_chunk_size(sizes, nodes, contract, log):
            data_roots = []
            for i, v in enumerate(sizes):
                log.info("submission %s, data size: %s", i, v)

                chunk_data = random.randbytes(v)
                submissions, data_root = create_submission(chunk_data)

                account_lock.acquire()
                account_idx = random.randint(0, len(accounts) - 1)
                contract.submit(submissions, tx_prarams=accounts[account_idx])
                account_lock.release()

                lock.acquire()
                client_idx = random.randint(0, len(nodes_index) - 1)
                idx = nodes_index[client_idx]
                nodes_index.pop(client_idx)
                lock.release()

                log.info("submit data via client %s", idx)
                wait_until(
                    lambda: nodes[idx].zgs_get_file_info(data_root) is not None
                )
                segment = submit_data(nodes[idx], chunk_data)
                wait_until(
                    lambda: nodes[idx].zgs_get_file_info(data_root)["finalized"]
                )

                lock.acquire()
                nodes_index.append(idx)
                lock.release()

                data_roots.append(data_root)

            for data_root in data_roots:
                for idx in range(len(nodes)):
                    wait_until(
                        lambda: nodes[idx].zgs_get_file_info(data_root) is not None
                    )

                    def wait_finalized():    
                        ret = nodes[idx].zgs_get_file_info(data_root)
                        if ret["finalized"]:
                            return True
                        else:
                            nodes[idx].admin_start_sync_file(ret['tx']['seq'])
                            return False

                    wait_until(
                        lambda: wait_finalized(), timeout = 180
                    )

        def run_small_chunk_size(nodes, contract, log):
            sizes = [i for i in range(1, SAMLL_SIZE + 1)]
            random.shuffle(sizes)

            run_chunk_size(sizes, nodes, contract, log)

        def run_large_chunk_size(nodes, contract, log):
            sizes = [i for i in range(256 * 1024 * 256 - LARGE_SIZE, 256 * 1024 * 256 )]
            random.shuffle(sizes)

            run_chunk_size(sizes, nodes, contract, log)

        def run_random_chunk_size(nodes, contract, log):
            sizes = []
            for i in range(RADMON_SIZE):
                sizes.append(random.randint(1, 256 * 1024 * 256))

            run_chunk_size(sizes, nodes, contract, log)

        t1 = threading.Thread(
            target=run_small_chunk_size, args=(self.nodes, self.contract, self.log)
        )
        t2 = threading.Thread(
            target=run_large_chunk_size, args=(self.nodes, self.contract, self.log)
        )
        t3 = threading.Thread(
            target=run_random_chunk_size, args=(self.nodes, self.contract, self.log)
        )

        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()


if __name__ == "__main__":
    FuzzTest().main()
