#!/bin/bash

#docker-compose down
#dcker network prune

N=100

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
cleanup
