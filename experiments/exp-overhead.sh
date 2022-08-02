#!/bin/bash

# Overhead Experiment

cd ../

rm results.csv
rm exp-overhead-results.csv

cargo build --release

# SmallBank, high contention, uniform mix, high serializable rate
for protocol in msgt sgt nocc; do
    ./target/release/spag -p $protocol -s 1 -b false -m high -c 40 -t $1 -w smallbank -d reduced
    sleep 5
done

# TATP, 100 entries, read committed, nurand
for protocol in msgt sgt nocc; do
    ./target/release/spag -p $protocol -s 1 -c 40 -t $1 -w tatp -d reduced
    sleep 5
done

# YCSB, 100K rows, 10 queries, 50% updates, 50% PL-3, high contention
for protocol in msgt sgt nocc; do
    ./target/release/spag -p $protocol -s 2 -c 40 -t $1 -w ycsb -d reduced -h 0.9 -u 0.5 -i 0.5 -q 10
    sleep 5
done

mv ./results.csv ./results/exp-overhead-results.csv

cd experiments
