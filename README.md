# ZeroG Storage

## Overview

ZeroG Storage is the storage layer for the ZeroGravity data availability (DA) system. The ZeroG Storage layer holds three important features:

* Buit-in - It is natively built into the ZeroGravity DA system for data storage and retrieval.
* General purpose - It is designed to support atomic transactions, mutable kv stores as well as archive log systems to enable wide range of applications with various data types.
* Incentive - Instead of being just a decentralized database, ZeroG Storage introduces PoRA mining algorithm to incentivize storage network participants.

To dive deep into the technical details, continue reading [ZeroG Storage Spec.](doc/)

## Integration

We provide a [SDK](https://github.com/zero-gravity-labs/js-zerog-storage-sdk) for users to easily integrate ZeroG Storage in their applications with the following features:

* File Merkle Tree Class
* Flow Contract Types
* RPC methods support
* File upload
* Support browser environment
* Tests for different environments (In Progress)
* File download (In Progress)

## Deployment

Please refer to [Deployment](doc/install.md) page for detailed steps to compile and start a ZeroG Storage node.

## Test

### Prerequisites

* Required python version: 3.8, 3.9, 3.10, higher version is not guaranteed (e.g. failed to install `pysha3`).
* Install dependencies under root folder: `pip3 install -r requirements.txt`

### Dependencies

Python test framework will launch blockchain fullnodes at local for storage node to interact with. There are 2 kinds of fullnodes supported:

* Conflux eSpace node (by default).
* BSC node (geth).

For Conflux eSpace node, the test framework will automatically compile the binary at runtime, and copy the binary to `tests/tmp` folder. For BSC node, the test framework will automatically download the latest version binary from [github](https://github.com/bnb-chain/bsc/releases) to `tests/tmp` folder.

Alternatively, you could also manually copy specific versoin binaries (conflux or geth) to the `tests/tmp` folder. Note, do **NOT** copy released conflux binary on github, since block height of some CIPs are hardcoded.

For testing, it's also dependent on the following repos:

* [ZeroG Storage Contract](https://github.com/zero-gravity-labs/zerog-storage-contracts): It essentially provides two abi interfaces for ZeroG Storage Node to interact with the on-chain contracts.
  * ZgsFlow: It contains apis to submit chunk data.
  * PoraMine: It contains apis to submit PoRA answers.
* [ZeroG Storage Client](https://github.com/zero-gravity-labs/zerog-storage-client): It is used to interact with certain ZeroG Storage Nodes to upload/download files.

### Run Tests

Go to the `tests` folder and run following command to run all tests:

```
python test_all.py
```

or, run any single test, e.g.

```
python sync_test.py
```

### Troubleshooting

1. Test failed due to blockchain full node rpc inaccessible.
   * Traceback: `node.wait_for_rpc_connection()`
   * Solution: unset the `http_proxy` and `https_proxy` environment variables if configured.

## Contributing

To make contributions to the project, please follow the guidelines [here](contributing.md).
