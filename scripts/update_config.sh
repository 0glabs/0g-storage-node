#!/bin/bash

MINER_KEY=""
MINE_CONTRACT=""
BLOCKCHAIN_RPC=""
FLOW_CONTRACT=""
BLOCK_NUMBER=0
PUBLIC_IP=$(curl -s https://ipinfo.io/ip)

FILE=run/config.toml

# enable sync
sed -in-place='' 's/# \[sync\]/\[sync\]/g' $FILE
# enable auto_sync
sed -in-place='' 's/# auto_sync_enabled = false/auto_sync_enabled = true/g' $FILE
# reduce timeout for finding peers
sed -in-place='' 's/# find_peer_timeout = "30s"/find_peer_timeout = "10s"/g' $FILE
# set public ip
sed -in-place='' "s/# network_listen_address = \"0.0.0.0\"/network_listen_address = \"$PUBLIC_IP\"/g" $FILE
# set miner key
sed -in-place='' "s/miner_key = \"\"/miner_key = \"$MINER_KEY\"/g" $FILE
# set miner contract address
sed -in-place='' "s/mine_contract_address = \"0x8B9221eE2287aFBb34A7a1Ef72eB00fdD853FFC2\"/mine_contract_address = \"$MINE_CONTRACT\"/g" $FILE
# set blockchain rpc endpoint
sed -in-place='' "s|blockchain_rpc_endpoint = \"https:\/\/rpc-testnet.0g.ai\"|blockchain_rpc_endpoint = \"$BLOCKCHAIN_RPC\"|g" $FILE
# set flow contract address
sed -in-place='' "s/log_contract_address = \"0x22C1CaF8cbb671F220789184fda68BfD7eaA2eE1\"/log_contract_address = \"$FLOW_CONTRACT\"/g" $FILE
# set contract deployed block number
sed -in-place='' "s/log_sync_start_block_number = 512567/log_sync_start_block_number = $BLOCK_NUMBER/g" $FILE
# update the boot node ids
sed -in-place='' 's|network_boot_nodes = .*|network_boot_nodes = ["/ip4/54.219.26.22/udp/1234/p2p/16Uiu2HAmTVDGNhkHD98zDnJxQWu3i1FL1aFYeh9wiQTNu4pDCgps","/ip4/52.52.127.117/udp/1234/p2p/16Uiu2HAkzRjxK2gorngB1Xq84qDrT4hSVznYDHj6BkbaE4SGx9oS"]|g' $FILE
