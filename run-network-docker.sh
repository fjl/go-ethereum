#!/usr/bin/env bash

if [[ -z "${N_NODES}" ]]; then
  N=1000
else
  N=${N_NODES}
fi

#N=1000

DIR=discv5-test

MIN_LATENCY_MS=10
MAX_LATENCY_MS=100

PORT=32000
RPC_PORT=22000
export COMPOSE_PARALLEL_LIMIT=1000
export COMPOSE_HTTP_TIMEOUT=300
export DOCKER_CLIENT_TIMEOUT=300

IP1=172
IP2=18
IP3=0

get_bootstrap_network() {
    ip=$IP1"."$IP2"."$IP3
    echo $ip
}

get_node_network() {
    IP3=$(($IP3+$1))
    while test $IP3 -gt 254
    do
      IP3=$((IP3-255))
      IP2=$((IP2+1))
    done

    while test $IP2 -gt 254
    do
      IP2=$((IP2-255))
      IP1=$((IP1+1))
    done
    ip=$IP1"."$IP2"."$IP3
    echo $ip
}

start_network() {
	docker network create -d bridge bootstrap-network --subnet=$(get_bootstrap_network).0/24 --gateway=$(get_bootstrap_network).1
	for i in $(seq $N); do
		docker network create -d bridge node$i-network --subnet=$(get_node_network $i).0/24 --gateway=$(get_node_network $i).1
	done
}

start_nodes() {
  mkdir -p $DIR/logs
  logfile="$DIR/logs/node-0.log"
  
  docker run --network bootstrap-network --cap-add=NET_ADMIN --name bootstrap-node --mount type=bind,source=$PWD/discv5-test,target=/go-ethereum/$DIR devp2p sh -c "./devp2p --verbosity 5 discv5 listen --bootnodes enode://582299339f1800f3ff3238ca8772b85543b5f5d4c1fab8d0a00c274a7bcf2cccb02ea72d250ee22b912d006b5a170c15c648cc2476d738baabb8519da7a7bd70@$(get_bootstrap_network).2:0?discport=$PORT --nodekey 7fbc0a865aad6ff63baf1d16e62c07e6cc7427d1f1fc99081af758d6aa27175b --addr $(get_bootstrap_network).2:$PORT --rpc $(get_bootstrap_network).2:$RPC_PORT 2>&1 | tee $logfile " &
  for i in $(seq $N); do

  	logfile="$DIR/logs/node-$i.log"
  	docker run --network node$i-network --cap-add=NET_ADMIN --name node$i --mount type=bind,source=$PWD/$DIR,target=/go-ethereum/$DIR devp2p sh -c "./devp2p --verbosity 5 discv5 listen --bootnodes enode://582299339f1800f3ff3238ca8772b85543b5f5d4c1fab8d0a00c274a7bcf2cccb02ea72d250ee22b912d006b5a170c15c648cc2476d738baabb8519da7a7bd70@$(get_bootstrap_network).2:0?discport=$PORT --addr $(get_node_network $i).2:$PORT --rpc $(get_node_network $i).2:$RPC_PORT 2>&1 | tee $logfile " &
	sleep 1
  done	  

}

config_network() {

  docker exec bootstrap-node sh -c "tc qdisc add dev eth0 root netem delay $(($MIN_LATENCY_MS + $RANDOM % $MAX_LATENCY_MS))ms" &
  for i in $(seq $N); do
  	docker exec node$i sh -c "tc qdisc add dev eth0 root netem delay $(($MIN_LATENCY_MS + $RANDOM % $MAX_LATENCY_MS))ms" &
        sleep 1
  done

}

cleanup() {
    docker stop bootstrap-node
    docker rm bootstrap-node 
    docker network rm bootstrap-network
    for i in $(seq $N); do
	docker stop node$i
	docker rm node$i
	 docker network rm node$i-network
    done	    
}

start_network

sudo iptables --flush DOCKER-ISOLATION-STAGE-1

trap cleanup EXIT

start_nodes
config_network

wait
