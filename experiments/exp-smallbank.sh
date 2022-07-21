#!/bin/bash

# SmallBank Experiment
# All serializable, high contention 

cd ../

rm results.csv
rm exp-smallbank-results.csv

cargo build --release

for protocol in msgt sgt; do
    for cores in 1 8 16 24 32 40 48; do
        ./target/release/spag -p $protocol -s 1 -c $cores -t $1 -w smallbank -m high -d reduced 
        sleep 5
    done
done

mv results.csv results/exp-smallbank-results.csv

cd experiments