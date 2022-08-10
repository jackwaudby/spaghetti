#!/bin/bash

cd ../

rm results.csv
rm exp-mc-whp-results.csv

cargo build --release

# smallbank
for protocol in sgt mcwhp; do
    for cores in 1 5 10 20 30 40; do
        ./target/release/spag --protocol $protocol --scalefactor 1 --cores $cores --workload smallbank --transactions $1
        sleep 5
    done
done

# tatp
for protocol in sgt mcwhp; do
    for cores in 1 5 10 20 30 40; do
        ./target/release/spag --protocol $protocol --scalefactor 1 --cores $cores --workload tatp --transactions $1
        sleep 5
    done
done

# ycsb
for protocol in sgt mcwhp; do
    for cores in 1 5 10 20 30 40; do
        ./target/release/spag --protocol $protocol --scalefactor 1 --cores $cores --workload ycsb --transactions $1 --theta 0.8
        sleep 5
    done
done

mv results.csv results/exp-mc-whp-results.csv

cd experiments
