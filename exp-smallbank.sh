#!/bin/bash

# SmallBank Experiment
# 20/60/20 RU/RC/S each txn random

rm results.csv
rm exp-smallbank-results.csv

cargo build --release

for protocol in msgt sgt; do
    for cores in 1 10 20 30 40; do
        ./target/release/spag -p $protocol -s 1 -c $cores -t $1 -w smallbank -d relevant
        sleep 5
    done
done

mv results.csv exp-smallbank-results.csv
