# Proof of Random Access

The ZeroGravity network adopts a Proof of Random Access (PoRA) mechanism to incentivize miners to store data. By requiring miners to answer randomly produced queries to archived data chunks, the PoRA mechanism establishes the relation between mining proof generation power and data storage. Miners answer the queries repeatedly and computes an output digest for each loaded chunk util find a digest that satisfies the mining difficulty (i.e., has enough leading zeros). PoRA will stress the miners' disk I/O and reduce their capability to respond user queries. So 0G Storage adopts intermittent mining, in which a mining epoch starts with a block generation at a specific block height on the host chain and stops when a valid PoRA is submitted to the 0G Storage contract.

In a strawman design, a PoRA iteration consists of a computing stage and a loading stage. In the computing stage, a miner computes a random recall position (the universal offset in the flow) based on an arbitrary picked random nonce and a mining status read from the host chain. In the loading stage, a miner loads the archived data chunks at the given recall position, and computes output digest by hashing the tuple of mining status and the data chunks. If the output digest satisfies the target difficulty, the miner can construct a legitimate PoRA consists of the chosen random nonce, the loaded data chunk and the proof for the correctness of data chunk to the mining contract.

## Fairness

The PoRA is designed with the following properties to improve the overall fairness in PoRA mining.

- Fairness for Small Miners
- Disincentivize Storage Outsourcing
- Disincentivize Distributed Mining

## Algorithm

Precisely, the mining process has the following steps:

1. Register the miner id on the mining contract
2. For each mining epoch, repeat the following steps:
   1. Wait for the layer-1 blockchain release a block at a given epoch height.
   2. Get the block hash $$\mathsf{block\_hash}$$ of this block and the relevant context (including $$\mathsf{merkle\_root}$$, $$\mathsf{data\_length}$$, $$\mathsf{context\_digest}$$) at this time.
   3. Compute the number of minable entries $$\text{n} = [\mathsf{data\_length}/256\mathsf{KB}]$$.
   4. For each iteration, repeat the following steps:
      1. Pick a random 32-byte $$\mathsf{nonce}$$.
      2. Decide the mining range parameters $$\mathsf{start\_position}$$ and $$\mathsf{mine\_length}$$; $$\mathsf{mine\_length}$$ should be equal to $$\text{min}(8\mathrm{TB}, n \times 256 \mathrm{KB})$$.
      3. Compute the recall position $$\tau$$ and the scratchpad $$\overrightarrow{s}$$ by the algorithm in Figure 1.
      4. Load the 256-kilobyte sealed data chunk $$\overrightarrow{d}$$ started from the position of $$h \cdot 256\mathrm{KB}$$.
      5. Compute $$\overrightarrow{w} = \overrightarrow{d}\ \mathtt{XOR}\ \overrightarrow{s}$$ and divide $$\overrightarrow{w}$$ into 64 4-kilobyte pieces.
      6. For each piece $$\overrightarrow{v}$$, compute the Blake2b hash of the tuple ($$\mathsf{miner\_id}$$, $$\mathsf{nonce}$$, $$\mathsf{context\_digest}$$, $$\mathsf{start\_position}$$, $$\mathsf{mine\_length}$$, $$\overrightarrow{v}$$).
      7. If one of Blake2b hash output is smaller than a target value, the miner finds a legitimate PoRA solution.

<figure><img src="../../../.gitbook/assets/zg-storage-algorithm.png" alt=""><figcaption><p>Figure 1. Recall Position and Scratchpat Computation</p></figcaption></figure>
