import os
import time
import shutil
import stat
import requests
import platform
from enum import Enum, unique

from utility.utils import is_windows_platform, wait_until

# v1.0.0-ci release
GITHUB_DOWNLOAD_URL="https://api.github.com/repos/0glabs/0g-storage-node/releases/152560136"

CONFLUX_BINARY = "conflux.exe" if is_windows_platform() else "conflux"
BSC_BINARY = "geth.exe" if is_windows_platform() else "geth"
ZG_BINARY = "0gchaind.exe" if is_windows_platform() else "0gchaind"
CLIENT_BINARY = "0g-storage-client.exe" if is_windows_platform() else "0g-storage-client"

ZG_GIT_REV = "7bc25a060fab9c17bc9942b6747cd07a668d3042" # v0.1.0
CLI_GIT_REV = "98d74b7e7e6084fc986cb43ce2c66692dac094a6"

@unique
class BuildBinaryResult(Enum):
    AlreadyExists = 0
    Installed = 1
    NotInstalled = 2

def build_conflux(dir: str) -> BuildBinaryResult:
    # Download or build conflux binary if absent
    result = __download_from_github(
        dir=dir,
        binary_name=CONFLUX_BINARY,
        github_url=GITHUB_DOWNLOAD_URL, 
        asset_name=__asset_name(CONFLUX_BINARY, zip=True),
    )

    if result == BuildBinaryResult.AlreadyExists or result == BuildBinaryResult.Installed:
        return result

    return __build_from_github(
        dir=dir,
        binary_name=CONFLUX_BINARY,
        github_url="https://github.com/Conflux-Chain/conflux-rust.git",
        build_cmd="cargo build --release --bin conflux",
        compiled_relative_path=["target", "release"],
    )

def build_bsc(dir: str) -> BuildBinaryResult:
    # Download bsc binary if absent
    result = __download_from_github(
        dir=dir,
        binary_name=BSC_BINARY,
        github_url="https://api.github.com/repos/bnb-chain/bsc/releases/79485895",
        asset_name=__asset_name(BSC_BINARY),
    )

    # Requires to download binary successfully, since it is not ready to build
    # binary from source code.
    assert result != BuildBinaryResult.NotInstalled, "Cannot download binary from github [%s]" % BSC_BINARY

    return result

def build_zg(dir: str) -> BuildBinaryResult:
    # Download or build 0gchain binary if absent
    result = __download_from_github(
        dir=dir,
        binary_name=ZG_BINARY,
        github_url=GITHUB_DOWNLOAD_URL,
        asset_name=__asset_name(ZG_BINARY, zip=True),
    )

    if result == BuildBinaryResult.AlreadyExists or result == BuildBinaryResult.Installed:
        return result

    return __build_from_github(
        dir=dir,
        binary_name=ZG_BINARY,
        github_url="https://github.com/0glabs/0g-chain.git",
        git_rev=ZG_GIT_REV,
        build_cmd="make install; cp $(go env GOPATH)/bin/0gchaind .",
        compiled_relative_path=[],
    )

def build_cli(dir: str) -> BuildBinaryResult:
    # Build 0g-storage-client binary if absent
    return __build_from_github(
        dir=dir,
        binary_name=CLIENT_BINARY,
        github_url="https://github.com/0glabs/0g-storage-client.git",
        git_rev=CLI_GIT_REV,
        build_cmd="go build",
        compiled_relative_path=[],
    )

def __asset_name(binary_name: str, zip: bool = False) -> str:
    sys = platform.system().lower()
    if sys == "linux":
        return f"{binary_name}_linux.zip" if zip else f"{binary_name}_linux"
    elif sys == "windows":
        binary_name = binary_name.removesuffix(".exe")
        return f"{binary_name}_windows.zip" if zip else f"{binary_name}_windows.exe"
    elif sys == "darwin":
        return f"{binary_name}_mac.zip" if zip else f"{binary_name}_mac"
    else:
        raise RuntimeError("Unable to recognize platform")

def __build_from_github(dir: str, binary_name: str, github_url: str, build_cmd: str, compiled_relative_path: list[str], git_rev = None) -> BuildBinaryResult:
    if git_rev is None:
        versioned_binary_name = binary_name
    elif binary_name.endswith(".exe"):
        versioned_binary_name = binary_name.removesuffix(".exe") + f"_{git_rev}.exe"
    else:
        versioned_binary_name = f"{binary_name}_{git_rev}"
    
    binary_path = os.path.join(dir, binary_name)
    versioned_binary_path = os.path.join(dir, versioned_binary_name)
    if os.path.exists(versioned_binary_path):
        __create_sym_link(versioned_binary_name, binary_name, dir)
        return BuildBinaryResult.AlreadyExists
    
    start_time = time.time()
    
    # clone code from github to a temp folder
    code_tmp_dir_name = (binary_name[:-4] if is_windows_platform() else binary_name) + "_tmp"
    code_tmp_dir = os.path.join(dir, code_tmp_dir_name)
    if os.path.exists(code_tmp_dir):
        shutil.rmtree(code_tmp_dir)
    os.system(f"git clone {github_url} {code_tmp_dir}")

    # build binary
    origin_path = os.getcwd()
    os.chdir(code_tmp_dir)
    if git_rev is not None:
        os.system(f"git checkout {git_rev}")
    os.system(build_cmd)

    # copy compiled binary to right place
    compiled_binary = os.path.join(code_tmp_dir, *compiled_relative_path, binary_name)
    shutil.copyfile(compiled_binary, versioned_binary_path)
    __create_sym_link(versioned_binary_name, binary_name, dir)

    if not is_windows_platform():
        st = os.stat(binary_path)
        os.chmod(binary_path, st.st_mode | stat.S_IEXEC)

    os.chdir(origin_path)

    shutil.rmtree(code_tmp_dir, ignore_errors=True)

    print("Completed to build binary " + binary_name + ", Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    return BuildBinaryResult.Installed

def __create_sym_link(src, dst, path = None):
    if src == dst:
        return
    
    origin_path = os.getcwd()
    if path is not None:
        os.chdir(path)

    if os.path.exists(dst):
        if os.path.isdir(dst):
            shutil.rmtree(dst)
        else:
            os.remove(dst)

    # Windows requires admin priviledge, use copy instead.
    if is_windows_platform():
        shutil.copy(src, dst)
    else:
        os.symlink(src, dst)

    os.chdir(origin_path)

def __download_from_github(dir: str, binary_name: str, github_url: str, asset_name: str) -> BuildBinaryResult:
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    binary_path = os.path.join(dir, binary_name)
    if os.path.exists(binary_path):
        return BuildBinaryResult.AlreadyExists
    
    print("Begin to download binary from github: %s" % binary_name, flush=True)
    
    start_time = time.time()

    req = requests.get(github_url)
    assert req.ok, "Failed to request: %s" % github_url
    download_url = None
    for asset in req.json()["assets"]:
        if asset["name"].lower() == asset_name:
            download_url = asset["browser_download_url"]
            break

    if download_url is None:
        print(f"Cannot find asset by name {asset_name}", flush=True)
        return BuildBinaryResult.NotInstalled
    
    content = requests.get(download_url).content

    # Supports to read from zipped binary
    if asset_name.endswith(".zip"):
        asset_path = os.path.join(dir, asset_name)
        with open(asset_path, "xb") as f:
            f.write(content)
        shutil.unpack_archive(asset_path, dir)
        assert os.path.exists(binary_path), f"Cannot find binary after unzip, binary = {binary_name}, asset = {asset_name}"
    else:
        with open(binary_path, "xb") as f:
            f.write(content)    

    if not is_windows_platform():
        st = os.stat(binary_path)
        os.chmod(binary_path, st.st_mode | stat.S_IEXEC)
    
    wait_until(lambda: os.access(binary_path, os.X_OK), timeout=120)

    print("Completed to download binary, Elapsed: " + str(int(time.time() - start_time)) + " seconds", flush=True)

    return BuildBinaryResult.Installed
