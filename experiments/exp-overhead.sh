#!/bin/bash

# Overhead Experiment

rm results.csv
rm exp-overhead-results.csv

cargo build --release

# SmallBank, high contention, uniform mix, high serializable rate
for protocol in msgt sgt nocc; do
    ./target/release/spag -p $protocol -s 1 -b false -m high -c 40 -t $1 -w smallbank -d reduced
    sleep 5
done

mv results.csv exp-overhead-results.csv
