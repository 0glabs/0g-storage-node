use ethers::prelude::abigen;

// run `cargo doc -p contract-interface --open` to read struct definition

abigen!(
    ZgsFlow,
    "../../0g-storage-contracts/artifacts/contracts/dataFlow/Flow.sol/Flow.json"
);

abigen!(
    PoraMine,
    "../../0g-storage-contracts/artifacts/contracts/test/PoraMineTest.sol/PoraMineTest.json"
);
