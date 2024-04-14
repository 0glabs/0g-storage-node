#!/bin/bash

set -e

EVMOSD=$(cd $(dirname ${BASH_SOURCE[0]})/../tmp; pwd)/evmosd
ROOT_DIR=${1:-.}
NUM_NODES=${2:-3}
P2P_PORT_START=${3:-26656}
CHAIN_ID=evmospy_9000-777

# install jq if not unavailable
jq --version >/dev/null 2>&1 || sudo snap install jq -y

mkdir -p $ROOT_DIR

# Init configs
for ((i=0; i<$NUM_NODES; i++)) do
	$EVMOSD init node$i --home $ROOT_DIR/node$i --chain-id $CHAIN_ID
	
	# Change parameter token denominations to aevmos
	GENESIS=$ROOT_DIR/node$i/config/genesis.json
	TMP_GENESIS=$ROOT_DIR/node$i/config/tmp_genesis.json
	cat $GENESIS | jq '.app_state["staking"]["params"]["bond_denom"]="aevmos"' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS
	cat $GENESIS | jq '.app_state["gov"]["params"]["min_deposit"][0]["denom"]="aevmos"' >$TMP_GENESIS && mv $TMP_GENESIS $GENESIS

    # Change app.toml
	APP_TOML=$ROOT_DIR/node$i/config/app.toml
	sed -i 's/minimum-gas-prices = "0aevmos"/minimum-gas-prices = "1aevmos"/' $APP_TOML
	sed -i '/\[json-rpc\]/,/^\[/ s/enable = false/enable = true/' $APP_TOML

	# Change config.toml
	CONFIG_TOML=$ROOT_DIR/node$i/config/config.toml
	sed -i '/seeds = /c\seeds = ""' $CONFIG_TOML
	sed -i 's/addr_book_strict = true/addr_book_strict = false/' $CONFIG_TOML
done

# Update persistent_peers in config.toml
for ((i=1; i<$NUM_NODES; i++)) do
	PERSISTENT_NODES=""
	for ((j=0; j<$i; j++)) do
		if [[ $j -gt 0 ]]; then PERSISTENT_NODES=$PERSISTENT_NODES,; fi
		NODE_ID=`$EVMOSD tendermint show-node-id --home $ROOT_DIR/node$j`
		P2P_PORT=$(($P2P_PORT_START+$j))
		PERSISTENT_NODES=$PERSISTENT_NODES$NODE_ID@127.0.0.1:$P2P_PORT
	done
	sed -i "/persistent_peers = /c\persistent_peers = \"$PERSISTENT_NODES\"" $ROOT_DIR/node$i/config/config.toml
done

# Create genesis with a single validator
$EVMOSD keys add val0 --keyring-backend test --home $ROOT_DIR/node0
$EVMOSD add-genesis-account val0 1000000000evmos --keyring-backend test --home $ROOT_DIR/node0

# add genesis account for tests, see GENESIS_PRIV_KEY and GENESIS_PRIV_KEY1 in node_config.py
$EVMOSD add-genesis-account evmos1l0j9dqdvd3fatfqywhm4y6avrln4jracaapkme 1000000000evmos --home $ROOT_DIR/node0
$EVMOSD add-genesis-account evmos1pemg6y3etj9tlhkl0vdwkrw36f74u2nlpxf6wc 1000000000evmos --home $ROOT_DIR/node0

mkdir -p $ROOT_DIR/gentxs
$EVMOSD gentx val0 500000000evmos --keyring-backend test --home $ROOT_DIR/node0 --output-document $ROOT_DIR/gentxs/node0.json
$EVMOSD collect-gentxs --home $ROOT_DIR/node0 --gentx-dir $ROOT_DIR/gentxs
$EVMOSD validate-genesis --home $ROOT_DIR/node0
for ((i=1; i<$NUM_NODES; i++)) do
    cp $ROOT_DIR/node0/config/genesis.json $ROOT_DIR/node$i/config/genesis.json
done
