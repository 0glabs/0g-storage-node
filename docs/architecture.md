# Architecture

## 0G System

ZeroGravity system consists of a data availability layer (0G DA) on top of a decentralized storage system (0G Storage). There is a separate consensus network that is part of both the 0G DA and the 0G Storage. For 0G Storage, the consensus is responsible for determining the ordering of the uploaded data blocks, realizing the storage mining verification and the corresponding incentive mechanism through smart contracts.

Figure 1 illustrates the architecture of the 0G system. When a data block enters the 0G DA, it is first erasure coded and organized into multiple consecutive chunks through erasure coding. The merkle root as a commitment of the encoded data block is then submitted to the consensus layer to keep the order of the data entering the system. The chunks are then dispersed to different storage nodes in 0G Storage where the data may be further replicated to other nodes depending on the storage fee that the user pays. The storage nodes periodically participate the mining process by interacting with the consensus network to accrue rewards from the system.

<figure><img src="../../.gitbook/assets/zg-storage-architecture.png" alt=""><figcaption><p>Figure 1. The Architecture of 0G System</p></figcaption></figure>

## 0G Storage

0G Storage employs layered design targeting to support different types of decentralized applications. Figure 2 shows the overview of the full stack layers of 0G Storage.

<figure><img src="../../.gitbook/assets/zg-storage-layer.png" alt=""><figcaption><p>Figure 2. Full Stack Solution of 0G Storage</p></figcaption></figure>

The lowest is a log layer that is a decentralized system. It consists of multiple storage nodes to form a storage network. The network has built-in incentive mechanism to reward the data storage. The ordering of the uploaded data is guaranteed by a sequencing mechanism to provide a log-based semantics and abstraction. This layer is used to store unstructured raw data for permanent persistency.

On top of the log layer, 0G Storage provides a Key-Value store runtime to manage structured data with mutability. Multiple key-value store nodes share the underlying log system. They put the structured key-value data structure into the log entry and append to the log system. They play the log entries in the shared log to construct the consistent state snapshot of the key-value store. The throughput and latency of the key-value store are bounded by the log system, so that the efficiency of the log layer is critical to the performance of the entire system. The key-value store can associate access control information with the keys to manage the update permission for the data. This enables the applications like social network, e.g., decentralized Twitter, which requires the maintenance for the ownership of the messages created by the users.

0G Storage further provides transactional semantics on the key-value store runtime to enable concurrent updates for the keys from multiple users who have the write access permission. The total order of the log entries guaranteed by the underlying log system provides the foundation for the concurrency control of the transactional executions on top of the key-value store. With this capability, 0G Storage can support decentralized applications like collaborative editing and even database workloads.

## Dependencies

The 0G Storage Node is depended by the [0G Storage KV](https://github.com/0glabs/0g-storage-kv). 0G Storage KV is essentially a wrapper layer on top of 0G Storage Node in order to provide mutable KV Store and transaction processing to applications. 0G DA uses the KV Store to store metadata of the data blobs.
