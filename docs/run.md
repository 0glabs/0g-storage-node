# Run

## Deploy contract: Token, Flow and Mine contracts

### Setup Environment

Install dependencies Node.js, yarn, hardhat.

- Linux

  - Ubuntu

  ```shell
  # node >=12.18
  sudo apt install npm
  sudo npm install --global yarn
  sudo npm install --global hardhat
  ```

- Mac

  ```shell
  brew install node
  sudo npm install --global yarn
  sudo npm install --global hardhat
  ```

- Windows  
  Download and install node from [here](https://nodejs.org/en/download/)
  ```shell
  npm install --global yarn
  npm install --global hardhat
  ```

### Download contract source code

```shell
git clone https://github.com/0glabs/0g-storage-contracts.git
cd 0g-storage-contracts
```

Add target network to your hardhat.config.js, i.e.

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

### Compile

```shell
yarn
yarn compile
```

### Deploy contract

```shell
npx hardhat run scripts/deploy.ts --network targetnetwork
```

Keep contracts addresses

## Run 0G Storage

Update config run/config.toml as required:

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

Run node

```shell
cd run
../target/release/zgs_node --config config.toml
```
