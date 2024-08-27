fn main() {
    if cfg!(not(feature = "dev")) {
        println!("cargo:rerun-if-changed=../../storage-contracts-abis/");
    } else {
        println!("cargo:rerun-if-changed=../../0g-storage-contracts-dev/artifacts/");
    }
}
