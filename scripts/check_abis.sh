
set -e

artifacts_path="$1"

check_abis() {
    for contract_name in "$@"; do
        diff $(./scripts/search_abi.sh "$artifacts_path" "$contract_name.json") "storage-contracts-abis/$contract_name.json"
    done
}
check_abis DummyMarket DummyReward Flow PoraMine PoraMineTest FixedPrice ChunkLinearReward FixedPriceFlow

