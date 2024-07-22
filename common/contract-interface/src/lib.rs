use ethers::prelude::abigen;

// run `cargo doc -p contract-interface --open` to read struct definition

#[cfg(not(feature = "dev"))]
abigen!(ZgsFlow, "../../storage-contracts-abis/Flow.json");

#[cfg(not(feature = "dev"))]
abigen!(PoraMine, "../../storage-contracts-abis/PoraMine.json");

#[cfg(feature = "dev")]
abigen!(
    ZgsFlow,
    "../../0g-storage-contracts-dev/artifacts/contracts/dataFlow/Flow.sol/Flow.json"
);

#[cfg(feature = "dev")]
abigen!(
    PoraMine,
    "../../0g-storage-contracts-dev/artifacts/contracts/miner/Mine.sol/PoraMine.json"
);
