#!/bin/bash
set -euo pipefail
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

cd $ROOT_DIR
echo "If you are running this script for the first time, please clean previous 
debug build first by running \`rm -rf target/debug\`."

# Install dependencies
cargo install grcov

# Build binary and run unit tests with code coverage.
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Cinline-threshold=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests"
export RUSTC_BOOTSTRAP=1

cargo build
cargo test --all

# Run python integration tests.
export ZGS="`pwd`/target/debug/zgs_node"
./tests/test_all.py

# Generate code coverage data
if [ -d "ccov" ]
then 
    rm -dr ccov
fi

mkdir ccov
zip -0 ccov/ccov.zip `find . \( -name "*.gc*" \) -print`
grcov ccov/ccov.zip -s . -t html --llvm --branch --ignore-not-existing --ignore "/*" \
--ignore "*target/debug/build/libp2p-*" \
--ignore "*target/debug/build/clang-sys*" \
--ignore "*target/debug/build/librocksdb-sys*" \
--ignore "*target/debug/build/solang*" -o ccov

echo "Code coverage result is saved to directory 'ccov'. 
You can open 'ccov/index.html' with a web brower to start."

