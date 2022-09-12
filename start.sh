#!/bin/bash

N=1
export COMPOSE_PARALLEL_LIMIT=1000
export COMPOSE_HTTP_TIMEOUT=300
export DOCKER_CLIENT_TIMEOUT=300

docker build . -f Dockerfile.topdisc -t devp2p
docker-compose up --scale devp2p=$N
