use ethers::prelude::abigen;

// run `cargo doc -p contract-interface --open` to read struct definition

abigen!(
    ZgsFlow,
    "../../zerog-storage-contracts/artifacts/contracts/dataFlow/Flow.sol/Flow.json"
);

abigen!(
    PoraMine,
    "../../zerog-storage-contracts/artifacts/contracts/test/PoraMineTest.sol/PoraMineTest.json"
);
