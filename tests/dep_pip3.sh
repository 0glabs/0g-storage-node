#!/bin/bash

set -e

function install() {
	if [ "`pip3 show ${1%%=*}`" =  "" ]; then
		pip3 install $1
	fi
}

install jsonrpcclient
install pyyaml
install pysha3
install coincurve
install eth_utils
install py-ecc
install web3
install eth_tester