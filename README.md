# 0G Storage

## Overview

0G Storage is a decentralized data storage system designed to address the challenges of high-throughput and low-latency data storage and retrieval, particularly for areas such as AI. 

## System Architecture

0G Storage consists of two main components:

1. **Data Publishing Lane**: Ensures fast Merkle tree data root commitment and verification through 0G Chain.
2. **Data Storage Lane**: Manages large data transfers and storage using an erasure-coding mechanism for redundancy and sharding for parallel processing.

Across the two lanes, 0G Storage supports the following features:

* **General Purpose Design**: Supports atomic transactions, mutable key-value stores, and archive log systems, enabling a wide range of applications with various data types.
* **Validated Incentivization**: Utilizes the PoRA (Proof of Random Access) mining algorithm to mitigate the data outsourcing issue and to ensure rewards are distributed to nodes who contribute to the storage network.

For in-depth technical details about 0G Storage, please read our [Intro to 0G Storage](https://docs.0g.ai/0g-storage).

## Documentation

- If you want to run a node, please refer to the [Running a Node](https://docs.0g.ai/run-a-node/storage-node) guide.
- If you want to conduct local testing, please refer to [Onebox Testing](https://github.com/0glabs/0g-storage-node/blob/main/docs/onebox-test.md) guide.
- If you want to build a project using 0G storage, please refer to the [0G Storage SDK](https://docs.0g.ai/build-with-0g/storage-sdk) guide.

## Support and Additional Resources
We want to do everything we can to help you be successful while working on your contribution and projects. Here you'll find various resources and communities that may help you complete a project or contribute to 0G. 

### Communities
- [0G Telegram](https://t.me/web3_0glabs)
- [0G Discord](https://discord.com/invite/0glabs)
