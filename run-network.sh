#!/usr/bin/env bash

set -euo pipefail

N_NODES=100
DIR=discv5-test
CONFIG_FILE=./discv5-stdconfig.json
JSONLOGS=1
QUIET=0

# Args processing.
while getopts "c:n:qJjh" arg; do
  case ${arg} in
    '?') exit 2 ;;
    h) echo "Usage: $0 [ -J ] [ -n netsize ] [ -c config.json ]"; exit 0 ;;
    j) JSONLOGS=1 ;;
    J) JSONLOGS=0 ;;
    q) QUIET=1 ;;
    n) N_NODES=$OPTARG ;;
    c) CONFIG_FILE=$OPTARG ;;
  esac
done

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
    ./devp2p key to-enr --ip 127.0.0.1 --tcp 0 --udp $port "$DIR/keys/node-$1.key"
}

# start_nodes launches the node processes.
start_nodes() {
    bootnode=$(get_node_url 1)
    echo "Bootstrap node: $bootnode"

    for i in $(seq $N_NODES); do
        let "port = 30200 + $i"
        let "rpc = 20200 + $i"
        keyfile="$DIR/keys/node-$i.key"
        logfile="$DIR/logs/node-$i.log"
        rm -f $logfile || true

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
        if [[ "$QUIET" = "1" ]]; then
            ./devp2p $logflags discv5 listen $nodeflags 2>"$logfile" >"$logfile" &
        else
            ./devp2p $logflags discv5 listen $nodeflags 2>&1 | tee "$logfile" &
        fi
    done
}

# write_experiment creates experiment.json
write_experiment() {
    rm -f $DIR/experiment.json
    cat >$DIR/experiment.json <<EOF
    {
        "nodes": ${N_NODES},
        "rpcBasePort": 20200,
        "config": $(cat $CONFIG_FILE)
    }
EOF
}

# Rebuild.
go build ./cmd/devp2p

# Generate all keys.
make_keys

# Initialize logs directory.
mkdir -p "$DIR/logs"
rm $DIR/logs/* || true
write_experiment

# Cleanup at exit.
trap 'kill $(jobs -p)' EXIT

# Launch nodes.
start_nodes
wait
