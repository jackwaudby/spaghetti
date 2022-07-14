#!/bin/bash

# Scalabilty Experiment
# cores = 1 to 40
# U = 0.5, omega = 0.2, theta = 0.8

rm results.csv

cargo build --release

for protocol in msgt sgt; do
    for cores in 1 10 20 30 40; do
        ./target/release/spag -p $protocol -s 1 -c $cores -t $1 -w ycsb -u 0.5 -i 0.2 -h 0.8 -d relevant
        sleep 5
    done
done
