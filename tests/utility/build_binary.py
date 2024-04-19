import os
import time
import shutil
import stat
import requests
import platform

from utility.utils import is_windows_platform, wait_until

def build_conflux(dir: str) -> bool:
    return __build_from_github(
        dir=dir,
        binary_name="conflux.exe" if is_windows_platform() else "conflux",
        github_url="https://github.com/Conflux-Chain/conflux-rust.git",
        build_cmd="cargo build --release --bin conflux",
        compiled_relative_path=["target", "release"],
    )

def build_bsc(dir: str) -> bool:
    sys = platform.system().lower()
    if sys == "linux":
        asset_name = "geth_linux"
    elif sys == "windows":
        asset_name = "geth_windows.exe"
    elif sys == "darwin":
        asset_name = "geth_mac"
    else:
        raise RuntimeError("Unable to recognize platform")

    return __download_from_github(
        dir=dir,
        binary_name="geth.exe" if is_windows_platform() else "geth",
        github_url="https://api.github.com/repos/bnb-chain/bsc/releases/79485895",
        asset_name=asset_name,
    )

def build_evmos(dir: str) -> bool:
    return __build_from_github(
        dir=dir,
        binary_name="evmosd.exe" if is_windows_platform() else "evmosd",
        github_url="-b testnet https://github.com/0glabs/0g-evmos.git",
        build_cmd="make install; cp $(go env GOPATH)/bin/evmosd .",
        compiled_relative_path=[],
    )


def __build_from_github(dir: str, binary_name: str, github_url: str, build_cmd: str, compiled_relative_path: list[str]) -> bool:
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    binary_path = os.path.join(dir, binary_name)
    if os.path.exists(binary_path):
        return False
    
    print("Begin to build binary from github: %s" % binary_name, flush=True)
    
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

    print("Completed to build binary, Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    return True

def __download_from_github(dir: str, binary_name: str, github_url: str, asset_name: str) -> bool:
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    binary_path = os.path.join(dir, binary_name)
    if os.path.exists(binary_path):
        return False
    
    print("Begin to download binary from github: %s" % binary_name, flush=True)
    
    start_time = time.time()

    with open(binary_path, "xb") as f:
        req = requests.get(github_url)
        assert req.ok, "Failed to request: %s" % github_url
        for asset in req.json()["assets"]:
            if asset["name"].lower() == asset_name:
                download_url = asset["browser_download_url"]
                break

        assert download_url is not None, "Cannot find binary to download by asset name [%s]" % asset_name

        f.write(requests.get(download_url).content)

        if not is_windows_platform():
            st = os.stat(binary_path)
            os.chmod(binary_path, st.st_mode | stat.S_IEXEC)
    
    wait_until(lambda: os.access(binary_path, os.X_OK), timeout=120)

    print("Completed to download binary, Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    return True
