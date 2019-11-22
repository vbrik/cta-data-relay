#!/bin/bash

test_name=$1

if [[ $test_name == "meta-set-gridftp" ]]; then
    bucket=cta-$test_name
    s3cmd rb --force --recursive s3://$bucket
    python3 -m cta_relay \
        --meta-set-gridftp --bucket $bucket \
        --gridftp-path /data/wipac/CTA/target5and7data/runs_320000_through_329999/
    s3cmd rb --force --recursive s3://$bucket
elif [[ $test_name == "meta-show" ]]; then
    python3 -m cta_relay --meta-show --bucket cta
fi
