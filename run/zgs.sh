#!/bin/bash

if [[ $# -eq 0 ]]; then
    echo "Usage: zgs.sh start | stop | info | clean"
    exit 1
fi

pid=`ps -ef | grep zgs_node | grep -v grep | awk '{print $2}'`

case $1 in

    start)
        if [[ "$pid" = "" ]]; then
            if [ ! -f .env ]; then
                echo ".env file not found"
            fi

            source .env

            if [[ "$ZGS_NODE__MINER_KEY" = "" ]]; then echo "ZGS_NODE__MINER_KEY not specified in .env file" fi
            if [[ "$ZGS_NODE__BLOCKCHAIN_RPC_ENDPOINT" = "" ]]; then echo "ZGS_NODE__BLOCKCHAIN_RPC_ENDPOINT not specified in .env file" fi

            nohup ../target/release/zgs_node --config config-testnet-turbo.toml \
                --log-config-file log_config_debug \
                --miner-key $ZGS_NODE__MINER_KEY \
                --blockchain-rpc-endpoint $ZGS_NODE__BLOCKCHAIN_RPC_ENDPOINT &

            echo "Storage node started ..."
        else
            echo "Storage node already started, pid = $pid"
            exit 1
        fi
        ;;

    stop)
        if [[ "$pid" = "" ]]; then
            echo "Storage node not started yet"
            exit 1
        else
            kill $pid
            echo "Storage node terminated, pid = $pid"
        fi
        ;;

    info)
        if [[ "$pid" = "" ]]; then
            echo "Storage node not started yet"
            exit 1
        else
            curl -X POST --data '{"jsonrpc":"2.0","method":"zgs_getStatus","params":[],"id":1}' \
                -H "Content-Type: application/json" http://127.0.0.1:5678 | jq -C ".result"
        fi
        ;;

    clean)
        rm -rf nohup.out db log
        ;;

    *)
        echo "Usage: zgs.sh start | stop | info | clean"
        ;;

esac
