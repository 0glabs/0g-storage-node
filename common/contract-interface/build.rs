fn main() {
    if cfg!(not(feature = "dev")) {
        println!("cargo:rerun-if-changed=../../0g-storage-contracts/artifacts/");
    } else {
        println!("cargo:rerun-if-changed=../../0g-storage-contracts-dev/artifacts/");
    }
}
