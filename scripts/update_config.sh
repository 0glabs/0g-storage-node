#!/bin/bash

MINER_KEY=""
MINE_CONTRACT="0x85F6722319538A805ED5733c5F4882d96F1C7384"
BLOCKCHAIN_RPC="https://rpc-testnet.0g.ai"
FLOW_CONTRACT="0x8873cc79c5b3b5666535C825205C9a128B1D75F1"
BLOCK_NUMBER=802
PUBLIC_IP=$(curl -s https://ipinfo.io/ip)

FILE=run/config.toml

# enable sync
sed -in-place='' 's/# \[sync\]/\[sync\]/g' $FILE
# enable auto_sync
sed -in-place='' 's/# auto_sync_enabled = false/auto_sync_enabled = true/g' $FILE
# reduce timeout for finding peers
sed -in-place='' 's/# find_peer_timeout = .*/find_peer_timeout = "10s"/g' $FILE
# set public ip
sed -in-place='' "s/# network_enr_address = .*/network_enr_address = \"$PUBLIC_IP\"/g" $FILE
# set miner key
sed -in-place='' "s/miner_key = \"\"/miner_key = \"$MINER_KEY\"/g" $FILE
# set miner contract address
sed -in-place='' "s/mine_contract_address = .*/mine_contract_address = \"$MINE_CONTRACT\"/g" $FILE
# set blockchain rpc endpoint
sed -in-place='' "s|blockchain_rpc_endpoint = .*|blockchain_rpc_endpoint = \"$BLOCKCHAIN_RPC\"|g" $FILE
# set flow contract address
sed -in-place='' "s/log_contract_address = .*/log_contract_address = \"$FLOW_CONTRACT\"/g" $FILE
# set contract deployed block number
sed -in-place='' "s/log_sync_start_block_number = .*/log_sync_start_block_number = $BLOCK_NUMBER/g" $FILE
# update the boot node ids
sed -in-place='' 's|network_boot_nodes = .*|network_boot_nodes = ["/ip4/54.219.26.22/udp/1234/p2p/16Uiu2HAmTVDGNhkHD98zDnJxQWu3i1FL1aFYeh9wiQTNu4pDCgps","/ip4/52.52.127.117/udp/1234/p2p/16Uiu2HAkzRjxK2gorngB1Xq84qDrT4hSVznYDHj6BkbaE4SGx9oS","/ip4/18.167.69.68/udp/1234/p2p/16Uiu2HAm2k6ua2mGgvZ8rTMV8GhpW71aVzkQWy7D37TTDuLCpgmX"]|g' $FILE
