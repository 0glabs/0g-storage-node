use std::process::Command;

const INSTALL_ERROR_MESSAGE: &str =
    "Install dependencies for contract fail, try to run `yarn` in folder 'zerog-storage-contracts'";
const COMPILE_ERROR_MESSAGE: &str =
    "Compile solidity contracts fail, try to run `yarn compile` in folder 'zerog-storage-contracts'";

fn main() {
    if cfg!(feature = "compile-contracts") {
        println!("cargo:rerun-if-changed=../../zerog-storage-contracts/contracts/");
        println!("cargo:rerun-if-changed=../../zerog-storage-contracts/hardhat.config.ts");

        let output = Command::new("yarn")
            .arg("--cwd")
            .arg("../../zerog-storage-contracts")
            .status()
            .expect(INSTALL_ERROR_MESSAGE);
        assert!(output.success(), "{}", INSTALL_ERROR_MESSAGE);

        let output = Command::new("yarn")
            .arg("--cwd")
            .arg("../../zerog-storage-contracts")
            .arg("compile")
            .status()
            .expect(COMPILE_ERROR_MESSAGE);
        assert!(output.success(), "{}", COMPILE_ERROR_MESSAGE);
    } else {
        println!("cargo:rerun-if-changed=../../zerog-storage-contracts/artifacts/");
    }
}
