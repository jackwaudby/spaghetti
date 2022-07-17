#!/bin/bash

# Isolation Experiment
# Omega = 0 to 1
# U = 0.5, theta = 0.8, cores = 40, queries = 10

cd ../

rm results.csv
rm exp-isolation-results.csv

cargo build --release

for protocol in msgt sgt; do
    for iso in 0.0 0.2 0.4 0.6 0.8 1; do
        ./target/release/spag -p $protocol -s 1 -c 40 -t $1 -w ycsb -u 0.5 -i $iso -h 0.8 -d reduced -q 10
        sleep 5
    done
done

mv results.csv exp-isolation-results.csv

cd experiments
