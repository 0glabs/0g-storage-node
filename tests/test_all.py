#!/usr/bin/env python3

import os

from utility.run_all import run_all

if __name__ == "__main__":
    run_all(
        test_dir = os.path.dirname(__file__),
        slow_tests={"mine_test.py", "random_test.py", "same_root_test.py"},
        long_manual_tests={"fuzz_test.py"},
        single_run_tests={"mine_with_market_test.py"},
    )