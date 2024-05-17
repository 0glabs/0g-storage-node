import argparse
import os
import time
import subprocess
import sys

from concurrent.futures import ProcessPoolExecutor

from utility.build_binary import build_conflux, build_bsc, build_zg, build_cli

DEFAULT_PORT_MIN = 11000
DEFAULT_PORT_MAX = 65535
DEFAULT_PORT_RANGE = 500

def print_testcase_result(color, glyph, script, start_time):
    print(color[1] + glyph + " Testcase " + script + "\telapsed: " + str(int(time.time() - start_time)) + " seconds" + color[0], flush=True)

def run_single_test(py, script, test_dir, index, port_min, port_max):
    try:
        # Make sure python thinks it can write unicode to its stdout
        "\u2713".encode("utf_8").decode(sys.stdout.encoding)
        TICK = "✓ "
        CROSS = "✖ "
        # CIRCLE = "○ "
    except UnicodeDecodeError:
        TICK = "P "
        CROSS = "x "
        # CIRCLE = "o "

    # BOLD, BLUE, RED, GREY = ("", ""), ("", ""), ("", ""), ("", "")
    BLUE, RED = ("", ""), ("", "")
    if os.name == "posix" or os.name == "nt":
        # primitive formatting on supported
        # terminal via ANSI escape sequences:
        # BOLD = ("\033[0m", "\033[1m")
        BLUE = ("\033[0m", "\033[0;34m")
        RED = ("\033[0m", "\033[0;31m")
        # GREY = ("\033[0m", "\033[1;30m")

    print("Running " + script, flush=True)

    port_min = port_min + (index * DEFAULT_PORT_RANGE) % (port_max - port_min)
    start_time = time.time()

    try:
        subprocess.check_output(
            args=[py, script, "--randomseed=1", f"--port-min={port_min}"],
            stdin=None,
            cwd=test_dir,
        )
    except subprocess.CalledProcessError as err:
        print_testcase_result(RED, CROSS, script, start_time)
        print("Output of " + script + "\n" + err.output.decode("utf-8"), flush=True)
        raise err
    print_testcase_result(BLUE, TICK, script, start_time)

def run_all(test_dir: str, test_subdirs: list[str]=[], slow_tests: set[str]={}, long_manual_tests: set[str]={}):
    tmp_dir = os.path.join(test_dir, "tmp")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)

    # Build blockchain binaries if absent
    build_conflux(tmp_dir)
    build_bsc(tmp_dir)
    build_zg(tmp_dir)
    build_cli(tmp_dir)

    start_time = time.time()

    parser = argparse.ArgumentParser(usage="%(prog)s [options]")
    parser.add_argument(
        "--max-workers",
        dest="max_workers",
        default=5,
        type=int,
    )
    parser.add_argument(
        "--port-max",
        dest="port_max",
        default=DEFAULT_PORT_MAX,
        type=int,
    )
    parser.add_argument(
        "--port-min",
        dest="port_min",
        default=DEFAULT_PORT_MIN,
        type=int,
    )
    options = parser.parse_args()

    TEST_SCRIPTS = []

    # include test_dir itself
    test_subdirs.insert(0, "")

    for subdir in test_subdirs:
        subdir_path = os.path.join(test_dir, subdir)
        for file in os.listdir(subdir_path):
            if file.endswith("_test.py"):
                rel_path = os.path.join(subdir, file)
                if rel_path not in slow_tests and rel_path not in long_manual_tests:
                    TEST_SCRIPTS.append(rel_path)

    executor = ProcessPoolExecutor(max_workers=options.max_workers)
    test_results = []

    py = "python3"
    if hasattr(sys, "getwindowsversion"):
        py = "python"

    i = 0
    # Start slow tests first to avoid waiting for long-tail jobs
    for script in slow_tests:
        f = executor.submit(
            run_single_test, py, script, test_dir, i, options.port_min, options.port_max
        )
        test_results.append((script, f))
        i += 1
    for script in TEST_SCRIPTS:
        f = executor.submit(
            run_single_test, py, script, test_dir, i, options.port_min, options.port_max
        )
        test_results.append((script, f))
        i += 1

    failed = set()
    for script, f in test_results:
        try:
            f.result()
        except subprocess.CalledProcessError as err:
            print("CalledProcessError " + repr(err))
            failed.add(script)

    print("Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    if len(failed) > 0:
        print("The following test fails: ")
        for c in failed:
            print(c)
        sys.exit(1)

