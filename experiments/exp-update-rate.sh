#!/bin/bash

# Update Rate Experiment
# cores = 40
# U = 0.0-1.0, omega = 0.5, theta = 0.6, queries = 10

cd ../

rm results.csv
rm exp-update-rate-results.csv

cargo build --release

for protocol in msgt sgt; do
    for ur in 0.0 0.2 0.4 0.6 0.8 1.0; do
        ./target/release/spag -p $protocol -s 2 -c 40 -t $1 -w ycsb -u $ur -i 0.2 -h 0.6 -d reduced -q 10
        sleep 5
    done
done

mv results.csv results/exp-update-rate-results.csv

cd experiments
