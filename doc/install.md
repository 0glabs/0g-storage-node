# Deployment

## Install

ZeroGStorage requires Rust 1.71.0 and Go to build.

### Install Rust

We recommend installing Rust through [rustup](https://www.rustup.rs/).

*   Linux

    Install Rust

    ```shell
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustup install 1.65.0
    ```

    Other dependencies

    *   Ubuntu

        ```shell
        sudo apt-get install clang cmake build-essential
        ```
*   Mac

    Install Rust

    ```shell
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustup install 1.65.0
    ```

    ```shell
    brew install llvm cmake
    ```
* Windows\
  Download and run the rustup installer from [this link](https://static.rust-lang.org/rustup/dist/i686-pc-windows-gnu/rustup-init.exe).\
  Install LLVM, pre-built binaries can be downloaded from [this link](https://releases.llvm.org/download.html).

### Install Go

*   Linux

    ```shell
    # Download the Go installer
    wget https://go.dev/dl/go1.19.3.linux-amd64.tar.gz

    # Extract the archive
    sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.19.3.linux-amd64.tar.gz

    # Add /usr/local/go/bin to the PATH environment variable by adding the following line to your ~/.profile.
    export PATH=$PATH:/usr/local/go/bin
    ```
*   Mac

    Download the Go installer from https://go.dev/dl/go1.19.3.darwin-amd64.pkg.\
    Open the package file you downloaded and follow the prompts to install Go.
* Windows\
  Download the Go installer from https://go.dev/dl/go1.19.3.windows-amd64.msi.\
  Open the MSI file you downloaded and follow the prompts to install Go.

### Build from source

```shell
# Download code
$ git clone https://github.com/zero-gravity-labs/zerog-storage-rust.git
$ cd zerog-storage-rust
$ git submodule update --init

# Build in release mode
$ cargo build --release
```

## Run

### Deploy contract: Token, Flow and Mine contracts

1. Install dependencies Node.js, yarn, hardhat.

* Linux
  *   Ubuntu

      ```
      # node >=12.18
      sudo apt install npm
      sudo npm install --global yarn
      sudo npm install --global hardhat
      ```
*   Mac

    ```shell
    brew install node
    sudo npm install --global yarn
    sudo npm install --global hardhat
    ```
*   Windows\
    Download and install node from [here](https://nodejs.org/en/download/)

    ```shell
    npm install --global yarn
    npm install --global hardhat
    ```

2. Download contract source code

```shell
git clone https://github.com/zero-gravity-labs/zerog-storage-contracts.git
cd zerog-storage-contracts
```

3. Add target network to your hardhat.config.js, i.e.

```shell
# example
networks: {
    targetNetwork: {
      url: "******",
      accounts: [
        "******",
      ],
    },
  },
```

4. Compile

```shell
yarn
yarn compile
```

5. Deploy contract

```shell
npx hardhat run scripts/deploy.ts --network targetnetwork
```

6. Keep contracts addresses

### Run ZeroG Storage Node

1. Update config run/config.toml as required:

```shell
# p2p port
network_libp2p_port

# rpc endpoint
rpc_listen_address

# peer nodes
network_libp2p_nodes

# flow contract address
log_contract_address

# mine contract address
mine_contract_address

# layer one blockchain rpc endpoint
blockchain_rpc_endpoint
```

2. Run node

```shell
cd run
../target/release/zgs_node --config config.toml
```
