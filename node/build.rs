use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../zerog-storage-client");

    let status = Command::new("go")
        .current_dir("../zerog-storage-client")
        .args(vec!["build", "-o", "../target"])
        .status()
        .unwrap();

    println!("build zerog-storage-client with status {}", status);
}
