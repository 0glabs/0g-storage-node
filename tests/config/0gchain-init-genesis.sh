#!/bin/bash

set -e

BINARY=$(cd $(dirname ${BASH_SOURCE[0]})/../tmp; pwd)/0gchaind
ROOT_DIR=${1:-.}
NUM_NODES=${2:-3}
P2P_PORT_START=${3:-26656}
CHAIN_ID=zgchainpy_9000-777

# install jq if not unavailable
jq --version >/dev/null 2>&1 || sudo snap install jq -y

mkdir -p $ROOT_DIR

SED_I="sed -i"
OS_NAME=`uname -o`
if [[ "$OS_NAME" = "Darwin" ]]; then
	SED_I="sed -i ''"
fi

# Init configs
for ((i=0; i<$NUM_NODES; i++)) do
	$BINARY init node$i --home $ROOT_DIR/node$i --chain-id $CHAIN_ID
	
	# Change genesis.json
	GENESIS=$ROOT_DIR/node$i/config/genesis.json
	TMP_GENESIS=$ROOT_DIR/node$i/config/tmp_genesis.json

	# Replace stake with neuron
	$SED_I 's/"stake"/"ua0gi"/g' "$GENESIS"

	# Replace the default evm denom of aphoton with neuron
	$SED_I 's/aphoton/neuron/g' "$GENESIS"

	cat $GENESIS | jq '.consensus_params.block.max_gas = "25000000"' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

	# Zero out the total supply so it gets recalculated during InitGenesis
	cat $GENESIS | jq '.app_state.bank.supply = []' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

	# Disable fee market
	cat $GENESIS | jq '.app_state.feemarket.params.no_base_fee = true' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

	# Disable london fork
	cat $GENESIS | jq '.app_state.evm.params.chain_config.london_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state.evm.params.chain_config.arrow_glacier_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state.evm.params.chain_config.gray_glacier_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state.evm.params.chain_config.merge_netsplit_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state.evm.params.chain_config.shanghai_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state.evm.params.chain_config.cancun_block = null' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

	# cat $GENESIS | jq '.app_state["staking"]["params"]["bond_denom"]="a0gi"' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	# cat $GENESIS | jq '.app_state["gov"]["params"]["min_deposit"][0]["denom"]="a0gi"' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

	cat "$GENESIS" | jq '.app_state["staking"]["params"]["max_validators"]=125' >"$TMP_GENESIS" && mv "$TMP_GENESIS" "$GENESIS"
	cat "$GENESIS" | jq '.app_state["slashing"]["params"]["signed_blocks_window"]="1000"' >"$TMP_GENESIS" && mv "$TMP_GENESIS" "$GENESIS"

	cat "$GENESIS" | jq '.app_state["consensus_params"]["block"]["time_iota_ms"]="3000"' >"$TMP_GENESIS" && mv "$TMP_GENESIS" "$GENESIS"

	# Change app.toml
	APP_TOML=$ROOT_DIR/node$i/config/app.toml
	$SED_I 's/minimum-gas-prices = "0ua0gi"/minimum-gas-prices = "1000000000neuron"/' $APP_TOML
	$SED_I '/\[grpc\]/,/^\[/ s/enable = true/enable = false/' $APP_TOML
	$SED_I '/\[grpc-web\]/,/^\[/ s/enable = true/enable = false/' $APP_TOML
	$SED_I '/\[json-rpc\]/,/^\[/ s/enable = false/enable = true/' $APP_TOML

	# Change config.toml
	CONFIG_TOML=$ROOT_DIR/node$i/config/config.toml
	# $SED_I '/seeds = /c\seeds = ""' $CONFIG_TOML
	$SED_I 's/addr_book_strict = true/addr_book_strict = false/' $CONFIG_TOML

	# Change block time to very small
	$SED_I 's/timeout_propose = "3s"/timeout_propose = "300ms"/' $CONFIG_TOML
	$SED_I 's/timeout_propose_delta = "500ms"/timeout_propose_delta = "50ms"/' $CONFIG_TOML
	$SED_I 's/timeout_prevote = "1s"/timeout_prevote = "100ms"/' $CONFIG_TOML
	$SED_I 's/timeout_prevote_delta = "500ms"/timeout_prevote_delta = "50ms"/' $CONFIG_TOML
	$SED_I 's/timeout_precommit = "1s"/timeout_precommit = "100ms"/' $CONFIG_TOML
	$SED_I 's/timeout_precommit_delta = "500ms"/timeout_precommit_delta = "50ms"/' $CONFIG_TOML
	$SED_I 's/timeout_commit = "5s"/timeout_commit = "500ms"/' $CONFIG_TOML
done

# Update persistent_peers in config.toml
for ((i=1; i<$NUM_NODES; i++)) do
	PERSISTENT_NODES=""
	for ((j=0; j<$i; j++)) do
		if [[ $j -gt 0 ]]; then PERSISTENT_NODES=$PERSISTENT_NODES,; fi
		NODE_ID=`$BINARY tendermint show-node-id --home $ROOT_DIR/node$j`
		P2P_PORT=$(($P2P_PORT_START+$j))
		PERSISTENT_NODES=$PERSISTENT_NODES$NODE_ID@127.0.0.1:$P2P_PORT
	done
	$SED_I "s/persistent_peers = \"\"/persistent_peers = \"$PERSISTENT_NODES\"/" $ROOT_DIR/node$i/config/config.toml
done

# Create genesis with a single validator
$BINARY keys add val0 --keyring-backend test --home $ROOT_DIR/node0
$BINARY add-genesis-account val0 15000000000000000000ua0gi --keyring-backend test --home $ROOT_DIR/node0

# add genesis account for tests, see GENESIS_PRIV_KEY and GENESIS_PRIV_KEY1 in node_config.py
$BINARY add-genesis-account 0g1l0j9dqdvd3fatfqywhm4y6avrln4jracmt6ztf 40000000000000000000ua0gi --home $ROOT_DIR/node0
$BINARY add-genesis-account 0g1pemg6y3etj9tlhkl0vdwkrw36f74u2nl8sjw7g 40000000000000000000ua0gi --home $ROOT_DIR/node0

mkdir -p $ROOT_DIR/gentxs
$BINARY gentx val0 10000000000000000000ua0gi --keyring-backend test --home $ROOT_DIR/node0 --output-document $ROOT_DIR/gentxs/node0.json
$BINARY collect-gentxs --home $ROOT_DIR/node0 --gentx-dir $ROOT_DIR/gentxs
$BINARY validate-genesis --home $ROOT_DIR/node0
for ((i=1; i<$NUM_NODES; i++)) do
	cp $ROOT_DIR/node0/config/genesis.json $ROOT_DIR/node$i/config/genesis.json
done
