# 0G Storage

## Organization

The [0G Storage repo](https://github.com/0glabs/0g-storage-node) is organized with two main modules, `common` and `node`, each with several submodules. `common` contains basic components needed for the `node` to run, while `node` contains key roles that compose the network.

## Directory structure

```
┌── : common
|   ├── : channel
|   ├── : directory
|   ├── : hashset_delay
|   ├── : lighthouse_metrics
|   ├── : merkle_tree
|   ├── : task_executor
|   ├── : zgs_version
|   ├── : append_merkle
|   └── : unused port
┌── : node
|   ├── : chunk_pool
|   ├── : file_location_cache
|   ├── : log_entry_sync
|   ├── : miner
|   ├── : network
|   ├── : router
|   ├── : rpc
|   ├── : shared_types
|   ├── : storage
|   ├── : storage async
|   └── : sync
├── : tests
```
