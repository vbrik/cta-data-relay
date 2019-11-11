#!/bin/bash

container=cta-relay
repo=vbrik

docker kill $container
sleep 1
docker build -t $repo/$container .
mkdir -p ~/.container_histories
touch ~/.container_histories/$container

docker run --detach -it --rm --name $container \
        -v ~/.container_histories/$container:/root/.bash_history \
        -v ~/.globus:/root/.globus \
        -v $PWD:/app \
        $repo/$container:latest
sleep 1
docker exec $container voms-proxy-init \
        -cert /root/.globus/cta-relay-cert.pem \
        -key /root/.globus/cta-relay-key.pem \
        -hours 72
