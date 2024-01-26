#!/bin/bash

# keep consistent with CI-lint in rust.yml
cargo fmt --all
cargo clippy -- -D warnings
