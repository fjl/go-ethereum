#!/usr/bin/env bash

N_NODES=3
DIR=discv5-test

CONFIG_FILE=./discv5-stdconfig.json
JSONLOGS=1

# Args processing.
while getopts ":c:Jjh" arg; do
  case ${arg} in
    h) echo "Usage: $0 [ -J ] [ -c config.json ]"; exit 0 ;;
    :) echo "$0: Must supply an argument to option -${OPTARG}." >&2; exit 1;;
    '?') echo "Invalid option: -${OPTARG}"; exit 2;;

    j) JSONLOGS=1 ;;
    J) JSONLOGS=0 ;;
    c) CONFIG_FILE="$OPTARG" ;;
  esac
done


rm  $DIR/logs/*

# make_keys creates all node keys.
make_keys() {
    mkdir -p $DIR/keys
    for i in $(seq $N_NODES); do
        file="$DIR/keys/node-$i.key"
        if [[ ! -f "$file" ]]; then
            echo "Generating $file"
            ./devp2p key generate "$file"
        fi
    done
}

# get_node_url returns the enode URL of node $i.
get_node_url() {
    let "port = 30200 + $1"
    ./devp2p key to-enode --tcp 0 --udp $port "$DIR/keys/node-$1.key"
}

# start_nodes launches the node processes.
start_nodes() {
    bootnode=$(get_node_url 1)
    echo "Bootstrap node: $bootnode"

    mkdir -p "$DIR/logs"

    for i in $(seq $N_NODES); do
        let "port = 30200 + $i"
        let "rpc = 20200 + $i"
        keyfile="$DIR/keys/node-$i.key"
        logfile="$DIR/logs/node-$i.log"
        rm -f "$logfile"

        logflags="--verbosity 5"
        if [[ "$JSONLOGS" = "1" ]]; then
            logflags="$logflags --log.json"
        fi

        nodeflags="--bootnodes $bootnode --nodekey $(cat $keyfile) --addr 127.0.0.1:$port --rpc 127.0.0.1:$rpc"
        if [[ -n "$CONFIG_FILE" ]]; then
            nodeflags="$nodeflags --config $CONFIG_FILE"
        fi

        # Start the node!
        echo "Starting node $i..."
        ./devp2p $logflags discv5 listen $nodeflags 2>&1 | tee "$logfile" &
    done
}

# Rebuild.
go build ./cmd/devp2p

# Generate all keys.
make_keys

# Cleanup at exit.
trap 'kill $(jobs -p)' EXIT

# Launch nodes.
start_nodes
wait
