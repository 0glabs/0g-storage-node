use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../0g-storage-client");

    let status = Command::new("go")
        .current_dir("../0g-storage-client")
        .args(vec!["build", "-o", "../target"])
        .status()
        .unwrap();

    println!("build 0g-storage-client with status {}", status);
}
