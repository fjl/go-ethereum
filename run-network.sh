#!/usr/bin/env bash

N_NODES=2
DIR=discv5-test
IP="172.19."
iface="tap"

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
    subnet="1."
    addr=$IP$subnet$1
    ./devp2p key to-enode "$DIR/keys/node-$1.key" --tcp 0 --udp $port --ip $addr
}

# start_nodes launches the node processes.
start_nodes() {
    bootnode=$(get_node_url 1)
    echo "Bootstrap node: $bootnode"

    mkdir -p "$DIR/logs"
    subnet=1
    sep="."
    host=1

    for i in $(seq $N_NODES); do
        let "port = 30200 + $i"
        let "rpc = 20200 + $i"
        if test $host -gt 254
        then
           host=0
           subnet=$((subnet+1))
        fi
        addr=$IP$subnet$sep$host
        echo $addr
 	keyfile="$DIR/keys/node-$i.key"
        logfile="$DIR/logs/node-$i.log"
        echo "Starting node $i..."
        rm -f "$logfile"
        ./devp2p --verbosity 5 discv5 listen --bootnodes "$bootnode" --nodekey "$(cat $keyfile)" --addr "$addr:$port" --rpc "$addr:$rpc" 2>&1 | tee "$logfile" &
        host=$((host+1))
    done
}

generate_ip_addresses(){

   brctl addbr br0
   ip link set br0 up
   subnet=1
   sep="."
   host=1
   for i in $(seq $N_NODES); do
        ifacename=$iface$i
        if test $host -gt 254
        then
           host=0
           subnet=$((subnet+1))
        fi
        addr=$IP$subnet$sep$host
        echo $ifacename $addr
        ip tuntap add mode tap $ifacename
        ip addr add $addr dev $ifacename
        ip link set $ifacename up
        brctl addif br0 $ifacename
        host=$((host+1))
   done

}

cleanup(){
    kill $(jobs -p)
    ip link del br0
    for i in $(seq $N_NODES); do
       ifacename=$iface$i
       ip link del $ifacename
    done
}

# Generate all keys.
make_keys

generate_ip_addresses

# Cleanup at exit.
trap cleanup EXIT

# Launch nodes.
start_nodes
wait
