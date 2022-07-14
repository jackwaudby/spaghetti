#!/bin/bash

# TATP Experiment
# All transactions run at RC
# NUrand on

rm results.csv

cargo build --release

for protocol in msgt sgt; do
    for cores in 1 10 20 30 40; do
        ./target/release/spag -p $protocol -s 1 -c $cores -t $1 -w tatp -d relevant
        sleep 5
    done
done
