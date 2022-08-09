#!/bin/bash

cd ../

rm results.csv
rm exp-mc-whp-results.csv

cargo build --release

for cores in 1 5 10 20 30 40; do
    ./target/release/spag --protocol mcwhp --scalefactor 1 --cores $cores --workload smallbank --transactions $1
    sleep 5
done

mv results.csv results/exp-mc-whp-results.csv

cd experiments
