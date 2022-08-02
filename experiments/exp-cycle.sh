#!/bin/bash

# Cycle Checking Experiment

cd ../

rm results.csv
rm exp-cycle-results.csv

cargo build --release

# for strategy in reduced restricted relevant; do
#     for cores in 1 10 20 30 40; do
#         ./target/release/spag -p msgt -s 2 -c $cores -t $1 -w ycsb -u 0.5 -i 0.2 -h 0.8 -d $strategy -q 10
#         sleep 5
#     done
# done

for strategy in reduced restricted relevant; do
    for iso in 0.0 0.2 0.4 0.6 0.8 1; do
        ./target/release/spag -p msgt -s 2 -c 40 -t $1 -w ycsb -u 0.5 -i $iso -h 0.8 -d $strategy -q 10
        sleep 5
    done
done

mv ./results.csv ./results/exp-cycle-results.csv

cd experiments
