import os
import time
import shutil
import stat

from utility.utils import is_windows_platform

CONFLUX_BINARY = "conflux.exe" if is_windows_platform() else "conflux"
EVMOS_BINARY = "evmosd.exe" if is_windows_platform() else "evmosd"

def build_conflux(dir: str) -> bool:
    build_from_github(
        dir=dir,
        binary_name=CONFLUX_BINARY,
        github_url="https://github.com/Conflux-Chain/conflux-rust.git",
        build_cmd="cargo build --release --bin conflux",
        compiled_relative_path=["target", "release"],
    )

def build_emvos(dir: str) -> bool:
    build_from_github(
        dir=dir,
        binary_name=EVMOS_BINARY,
        github_url="-b testnet https://github.com/0glabs/0g-evmos.git",
        build_cmd="make install; cp $(go env GOPATH)/bin/evmosd .",
        compiled_relative_path=[],
    )

def build_from_github(dir: str, binary_name: str, github_url: str, build_cmd: str, compiled_relative_path: list[str]) -> bool:
    binary_path = os.path.join(dir, binary_name)
    if os.path.exists(binary_path):
        return False
    
    start_time = time.time()
    
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

    print("Completed to build binary " + binary_name + ", Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    return True
