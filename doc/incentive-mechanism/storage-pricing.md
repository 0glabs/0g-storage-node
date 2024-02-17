# Storage Pricing

The cost of each 0G Storage request is composed of two parts: fee and storage endowment. The fee part is paid to host chain miners/validators for invoking the ZeroGravity contract to process storage request and add new data entry into the log, which is priced as other smart contract invocation transactions. In what follows we focus on the storage endowment part, which supports the perpetual reward to 0G Storage miners who serve the corresponding data.&#x20;

Given a data storage request $SR$ with specific amount of endowment $SR_{endowment}$ and size of committed data $SR_{data}$ size (measured in number of 256 B sectors), the unit price of $SR$ is calculated as follows:

$$ SR_{unit\_price} = {SR_{endowment} \over SR_{data\_size}} $$

This unit price $SR_{unit}$ price must exceed a globally specified lower bound to be added to the log, otherwise the request will be pending until when the lower bound decreased below $SR_{unit}$ price (in the meanwhile miners will most likely not store these unpaid data). Users are free to set a higher unit price $SR_{unit}$ price, which would motivate more storage nodes mining on that data entry and hence lead to better data availability.
