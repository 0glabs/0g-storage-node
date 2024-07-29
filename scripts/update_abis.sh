#!/bin/bash

set -e

default_path="../0g-storage-contracts"
path="${1:-$default_path}"

# Step 1: Check if the path is a valid Git directory with commits
if [ ! -d "$path/.git" ] || [ -z "$(git -C "$path" rev-parse HEAD 2> /dev/null)" ]; then
    echo "Error: The specified path is not a valid Git repository with commits."
    exit 1
fi
if [ ! -z "$(git -C "$path" status --porcelain)" ]; then
    echo "Error: There are uncommitted changes in the contract repository."
    exit 1
fi

# Step 2: Build the contracts
build_contracts() {
    local target_path="$path"
    local original_path=$(pwd)  # Save the current directory

    if cd "$target_path"; then
        yarn
        yarn build
        cd "$original_path"
    else
        echo "Error: Failed to switch to directory $target_path."
        exit 1
    fi
}

build_contracts

# Step 3: Copy the file from a specified sub-path
copy_file() {
    local source_path="$1"
    local destination_path="$2"

    # Check if the source file exists
    if [ ! -f "$source_path" ]; then
        echo "Error: The file $source_path does not exist."
        exit 1
    fi

    # Copy the file to the destination
    cp "$source_path" "$destination_path"
    echo "File copied: $source_path -> $destination_path."
}

copy_abis() {
    for contract_name in "$@"; do
        copy_file $(./scripts/search_abi.sh "$path/artifacts" "$contract_name.json") "storage-contracts-abis/$contract_name.json"
    done
}

copy_abis DummyMarket DummyReward Flow PoraMine PoraMineTest FixedPrice ChunkLinearReward FixedPriceFlow


# Step 4: Get the current Git revision and write it to a specified file
git_revision=$(git -C "$path" rev-parse HEAD)
revision_file="storage-contracts-abis/0g-storage-contracts-rev"
echo "$git_revision" > "$revision_file"

echo "Write git rev $git_revision to $revision_file."
