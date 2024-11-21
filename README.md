# 0G Storage

## Overview

0G Storage is the storage layer for the ZeroGravity data availability (DA) system. The 0G Storage layer holds three important features:

* Built-in - It is natively built into the ZeroGravity DA system for data storage and retrieval.
* General purpose - It is designed to support atomic transactions, mutable kv stores as well as archive log systems to enable wide range of applications with various data types.
* Incentive - Instead of being just a decentralized database, 0G Storage introduces PoRA mining algorithm to incentivize storage network participants.

To dive deep into the technical details, continue reading [0G Storage Spec.](docs/)

## Integration

We provide a [SDK](https://github.com/0glabs/0g-js-storage-sdk) for users to easily integrate 0G Storage in their applications with the following features:

* File Merkle Tree Class
* Flow Contract Types
* RPC methods support
* File upload
* Support browser environment
* Tests for different environments (In Progress)
* File download (In Progress)

## Deployment

Please refer to [Deployment](docs/run.md) page for detailed steps to compile and start a 0G Storage node.

## Test

Please refer to the [One Box Test](docs/onebox-test.md) page for local testing purpose.

## Contributing

To make contributions to the project, please follow the guidelines [here](contributing.md).
