#!/usr/bin/env bash

N_NODES=3
DIR=discv5-test

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
    ./devp2p key to-enode "$DIR/keys/node-$1.key" --tcp 0 --udp $port
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
        echo "Starting node $i..."
        rm -f "$logfile"
        ./devp2p --verbosity 5 discv5 listen --bootnodes "$bootnode" --nodekey "$(cat $keyfile)" --addr "127.0.0.1:$port" --rpc "127.0.0.1:$rpc" | tee "$logfile" &
    done
}

# Generate all keys.
make_keys

# Cleanup at exit.
trap 'kill $(jobs -p)' EXIT

# Launch nodes.
start_nodes
wait
