# Mining Reward

0G Storage creates pricing segments every 8 GB of data chunks over the data flow. Each pricing segment is associated with an Endowment Pool and a Reward Pool. The Endowment Pool collects the storage endowments of all the data chunks belongs to this pricing segment and releases a fixed ratio of balance to the Reward Pool every second. The rate of reward release is set to 4% per year.

The mining reward is paid to miners for providing data service. Miners receive mining reward when submit the first legitimate PoRA for a mining epoch to 0G Storage contract. The mining reward consists of two parts:

The mining reward consists of two parts:

* Base reward: the base reward, denoted by $$R_{base}$$, is paid for every accepted mining proof. The base reward per proof decreases over time.
* Storage reward: the storage reward, denoted by $$R_{storage}$$, is the perpetual reward from storing data. When a PoRA falls in a pricing segment, half of the balance in its Reward Pool are claimed as the storage reward.

The total reward for a new mining proof is thus: $$R_{total} = R_{base} + R_{storage}$$
