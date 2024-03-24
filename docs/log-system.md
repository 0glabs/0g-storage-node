# Log System

The log layer of 0G Storage provides decentralized storage service via a permissionless network of storage nodes. These storage nodes collaboratively serve archived data, where each node optionally specifies which portion of data it keeps in local storage.

## Protocol

The storage state of 0G Storage network is maintained in a smart contract deployed on an existing blockchain. The design of 0G Storage network fully decouples data creation, reward distribution, and token circulation.

The 0G Storage Contract is responsible for data storage requests processing, data entries creation, and reward distribution.

- Data storage requests are submitted by users who wish to store data in the 0G Storage network, where each request includes necessary metadata such as data size and commitments, and it comes along with the payment for storage service.
- Data entries are created for accepted data requests, to keep record of stored data.
- Reward distribution is handled independently through a mining process. Storage nodes submit mining proofs to the 0G Storage contract to claim rewards for maintaining the 0G Storage network. The token circulation of 0G is fully embedded into the host chain ecosystem, as an ERC20 token maintained by another contract called the ZG ledger.

This embedding design brings significant advantages:

- Simplicity: there is no need to maintain a full-fledged consensus protocol, which reduces complexity and enables 0G Storage to focus on decentralized storage service.
- Safety: the consensus is outsourced to the host blockchain, and hence inherits security of the host blockchain. Typically the more developed host blockchain would provide stronger safety guarantee than a newly-built blockchain.
- Accessibility: every smart contract on the host blockchain is able to access the original state of ZeroGravity directly, without relying on some trusted off-chain notary. This difference is essential comparing to the projection of an external ledger managed by a third-party.
- Composability: 0G tokens can always be transferred directly on the host blockchain, like any other ERC20 tokens. This is much more convenient than typical layer-2 ledgers, where transactions are  first processed by layer-2 validators and then committed to the host chain after a significant latency. This feature empowers 0G Storage stronger composability as a new lego to the ecosystem.

## Storage Granularity

The log layer of 0G Storage is updated (append-only) at the granularity of log entries, where every entry is created by a storage-request transaction sent to the 0G Storage contract. When realizing the log layer as a filesystem, every log entry corresponds to a file. The log system is addressed at the level of fixed-size sectors, where each sector stores 256 B of data. To avoid the case that one sector is shared by distinct log entries, every log entry must be padded to a multiple of sectors.

The mining process of 0G Storage requires to prove data accessibility to random challenge queries. To maximize the competitive edge of SSD storage, the challenge queries are set to the level of 256 KB chunks, i.e. 1024 sectors. That is, every challenge query requires the miner to prove accessibility to a whole chunk of data. Therefore storage nodes would maintain data at the granularity of chunks.

## Data Flow

In 0G Storage, committed data are organized sequentially. Such a sequence of data is called a data flow, which can be interpreted as a list of data entries or equivalently a sequence of fixed-size data sectors. Thus, every piece of data in ZeroGravity can be indexed conveniently with a universal offset. This offset will be used to sample challenges in the mining process of PoRA. The default data flow is called the "main flow" of ZeroGravity. It incorporates all new log entries (unless otherwise specified) in an append-only manner. There are also specialized flows that only accept some category of log entries, e.g. data related to a specifc application. The most significant advantage of specialized flows is a consecutive addressing space, which may be crucial in some use cases. Furthermore, a specialized flow can apply customized storage price, which is typically significantly higher than the floor price of the default flow, and hence achieves better data availability and reliability.
