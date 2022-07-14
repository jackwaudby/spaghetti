#!/bin/bash

# Contention Experiment
# theta = 0.6 to 0.9
# U = 0.5, omega = 0.2, cores = 40

rm results.csv

cargo build --release

for protocol in msgt sgt; do
    for con in 0.6 0.7 0.75 0.8 0.85 0.9; do
        ./target/release/spag -p $protocol -s 1 -c 40 -t $1 -w ycsb -u 0.5 -i 0.2 -h $con -d relevant
        sleep 5
    done
done