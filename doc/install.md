# Install
ZeroGStorage requires Rust 1.71.0 and Go to build.

## Install Rust

We recommend installing Rust through [rustup](https://www.rustup.rs/).

* Linux

    Install Rust
    ```shell
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustup install 1.65.0
    ```

    Other dependencies
    * Ubuntu
        ```shell
        sudo apt-get install clang cmake build-essential
        ```

* Mac

    Install Rust
    ```shell
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustup install 1.65.0
    ```

    ```shell
    brew install llvm cmake
    ```

* Windows  
    Download and run the rustup installer from [this link](https://static.rust-lang.org/rustup/dist/i686-pc-windows-gnu/rustup-init.exe).  
    Install LLVM, pre-built binaries can be downloaded from [this link](https://releases.llvm.org/download.html).

## Install Go
* Linux
    ```shell
    # Download the Go installer
    wget https://go.dev/dl/go1.19.3.linux-amd64.tar.gz

    # Extract the archive
    sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.19.3.linux-amd64.tar.gz

    # Add /usr/local/go/bin to the PATH environment variable by adding the following line to your ~/.profile.
    export PATH=$PATH:/usr/local/go/bin
    ```

* Mac

    Download the Go installer from https://go.dev/dl/go1.19.3.darwin-amd64.pkg.  
    Open the package file you downloaded and follow the prompts to install Go.

* Windows  
    Download the Go installer from https://go.dev/dl/go1.19.3.windows-amd64.msi.  
    Open the MSI file you downloaded and follow the prompts to install Go.


## Build from source
```shell
# Download code
$ git clone https://github.com/0glabs/0g-storage-node.git
$ cd 0g-storage-node
$ git submodule update --init

# Build in release mode
$ cargo build --release
```
