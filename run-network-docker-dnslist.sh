#!/usr/bin/env bash

#DNSLISTPATH="/home/sergi/workspace/eth-dns-list-parser/target"
#DNSLISTEXE="java -cp $DNSLISTPATH/discv4-dnslist-parser-0.0.1-SNAPSHOT.jar  network.datahop.DnsListParser"

nets=()

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
IP2=21
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

start_bootstrap_network() {
	docker network create -d bridge bootstrap-network --subnet=$(get_bootstrap_network).0/24 --gateway=$(get_bootstrap_network).1
	sleep 1
}

start_network() {
	docker network create -d bridge network-$1.0 --subnet=$1.0/24 --gateway=$1.1
	sudo iptables --flush DOCKER-ISOLATION-STAGE-1
}

start_bootstrap_node() {
  mkdir -p $DIR/logs
  logfile="$DIR/logs/node-0.log"
  
  docker run --network bootstrap-network --cap-add=NET_ADMIN --name bootstrap-node --mount type=bind,source=$PWD/discv5-test,target=/go-ethereum/$DIR devp2p sh -c "./devp2p --verbosity 5 discv5 listen --bootnodes enode://582299339f1800f3ff3238ca8772b85543b5f5d4c1fab8d0a00c274a7bcf2cccb02ea72d250ee22b912d006b5a170c15c648cc2476d738baabb8519da7a7bd70@$(get_bootstrap_network).2:0?discport=$PORT --nodekey 7fbc0a865aad6ff63baf1d16e62c07e6cc7427d1f1fc99081af758d6aa27175b --addr $(get_bootstrap_network).2:$PORT --rpc $(get_bootstrap_network).2:$RPC_PORT 2>&1 | tee $logfile " &

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
	echo "node$i"
	docker stop node$i
	docker rm node$i
    done
    for i in "${nets[@]}"
    do
    	docker network rm $i
    done    
}


load_dnslist(){
  output=$($DNSLISTEXE)
  i=0
  addr=""
  n=0
  for x in $output; 
  do 
     if [ $n -eq $N ];
     then
	echo "$N nodes created"
	break
     fi
     if [ $i -eq 0 ];
     then
	addr=$x
        start_network $addr
	i=-1
     elif [ $i -eq -1 ]; 
     then
        i=$x
     else
	i=$((i-1))
	logfile="$DIR/logs/node-$n.log"
	n=$((n+1))
        docker run --network network-$addr.0 --ip $x --cap-add=NET_ADMIN --name node$n --mount type=bind,source=$PWD/$DIR,target=/go-ethereum/$DIR devp2p sh -c "./devp2p --verbosity 5 discv5 listen --bootnodes enode://582299339f1800f3ff3238ca8772b85543b5f5d4c1fab8d0a00c274a7bcf2cccb02ea72d250ee22b912d006b5a170c15c648cc2476d738baabb8519da7a7bd70@$(get_bootstrap_network).2:0?discport=$PORT --addr $x:$PORT --rpc $x:$RPC_PORT 2>&1 | tee $logfile " &
	nets+=(network-$addr.0)
     fi
  done;

}

start_bootstrap_network

start_bootstrap_node

load_dnslist

trap cleanup EXIT


wait
