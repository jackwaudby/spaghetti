#!/bin/bash

# Scalabilty Experiment
# cores = 1 to 40
# U = 0.5, omega = 0.2, theta = 0.8, queries = 10

cd ../

rm results.csv
rm exp-scalability-results.csv

cargo build --release

for protocol in msgt sgt; do
    for cores in 1 10 20 30 40; do
        ./target/release/spag -p $protocol -s 2 -c $cores -t $1 -w ycsb -u 0.5 -i 0.2 -h 0.8 -d reduced -q 10
        sleep 5
    done
done

mv results.csv results/exp-scalability-results.csv

cd experiments
