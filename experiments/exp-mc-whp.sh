#!/bin/bash

cd ../

rm results.csv
rm exp-mc-whp-results.csv

cargo build --release

for scalefactor in 1 2 3; do
    for protocol in sgt mcwhp; do
        for cores in 1 5 10 20 30 40; do
            ./target/release/spag --protocol $protocol --scalefactor $scalefactor --cores $cores --workload smallbank --transactions $1
            sleep 5
        done
    done
done

mv results.csv results/exp-mc-whp-results.csv

cd experiments
