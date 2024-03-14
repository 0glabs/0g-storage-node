use std::process::Command;

const INSTALL_ERROR_MESSAGE: &str =
    "Install dependencies for contract fail, try to run `yarn` in folder '0g-storage-contracts'";
const COMPILE_ERROR_MESSAGE: &str =
    "Compile solidity contracts fail, try to run `yarn compile` in folder '0g-storage-contracts'";

#[allow(dead_code)]
fn compile_contracts() {
    println!("cargo:rerun-if-changed=../../0g-storage-contracts/contracts/");
    println!("cargo:rerun-if-changed=../../0g-storage-contracts/hardhat.config.ts");

    let output = Command::new("yarn")
        .arg("--cwd")
        .arg("../../0g-storage-contracts")
        .status()
        .expect(INSTALL_ERROR_MESSAGE);
    assert!(output.success(), "{}", INSTALL_ERROR_MESSAGE);

    let output = Command::new("yarn")
        .arg("--cwd")
        .arg("../../0g-storage-contracts")
        .arg("compile")
        .status()
        .expect(COMPILE_ERROR_MESSAGE);
    assert!(output.success(), "{}", COMPILE_ERROR_MESSAGE);
}

fn main() {
    if cfg!(feature = "compile-contracts") {
        // compile_contracts();
        // return;
    }
    println!("cargo:rerun-if-changed=../../0g-storage-contracts/artifacts/");
}
