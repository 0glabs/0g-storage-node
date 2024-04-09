import argparse
import os
import time
import subprocess
import sys
import shutil
import stat

from concurrent.futures import ProcessPoolExecutor

from utility.utils import is_windows_platform

DEFAULT_PORT_MIN = 11000
DEFAULT_PORT_MAX = 65535
DEFAULT_PORT_RANGE = 500

CONFLUX_BINARY = "conflux.exe" if is_windows_platform() else "conflux"
EVMOS_BINARY = "evmosd.exe" if is_windows_platform() else "evmosd"

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
    start_time = time.time()

    tmp_dir = os.path.join(test_dir, "tmp")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)

    # Build conflux binary if absent
    build_from_github(
        dir=tmp_dir,
        binary_name=CONFLUX_BINARY,
        github_url="https://github.com/Conflux-Chain/conflux-rust.git",
        build_cmd="cargo build --release --bin conflux",
        compiled_relative_path=["target", "release"],
    )

    # Build evmos binary if absent
    build_from_github(
        dir=tmp_dir,
        binary_name=EVMOS_BINARY,
        github_url="https://github.com/0glabs/0g-evmos.git",
        build_cmd="make install; cp $(go env GOPATH)/bin/evmosd .",
        compiled_relative_path=[],
    )

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

def build_from_github(dir: str, binary_name: str, github_url: str, build_cmd: str, compiled_relative_path: list[str]) -> bool:
    binary_path = os.path.join(dir, binary_name)
    if os.path.exists(binary_path):
        return False
    
    # clone code from github to a temp folder
    code_tmp_dir_name = (binary_name[:-4] if is_windows_platform() else binary_name) + "_tmp"
    code_tmp_dir = os.path.join(dir, code_tmp_dir_name)
    if os.path.exists(code_tmp_dir):
        shutil.rmtree(code_tmp_dir)
    clone_command = "git clone " + github_url + " " + code_tmp_dir
    os.system(clone_command)

    # build binary
    origin_path = os.getcwd()
    os.chdir(code_tmp_dir)
    os.system(build_cmd)

    # copy compiled binary to right place
    compiled_binary = os.path.join(code_tmp_dir, *compiled_relative_path, binary_name)
    shutil.copyfile(compiled_binary, binary_path)

    if not is_windows_platform():
        st = os.stat(binary_path)
        os.chmod(binary_path, st.st_mode | stat.S_IEXEC)

    os.chdir(origin_path)

    shutil.rmtree(code_tmp_dir, ignore_errors=True)

    return True
