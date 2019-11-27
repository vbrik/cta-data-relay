#!/bin/bash

container=cta-data-relay

docker kill $container
sleep 1
docker build -t $container .
mkdir -p ~/.container_histories
touch ~/.container_histories/$container

docker run --detach -it --rm --name $container \
        -v ~/.container_histories/$container:/root/.bash_history \
        -v ~/.globus:/root/.globus \
        -v $PWD:/app \
        $container:latest
sleep 1
docker exec $container voms-proxy-init \
        -cert /root/.globus/cta-data-relay-cert.pem \
        -key /root/.globus/cta-data-relay-key.pem \
        -hours 72
