#!/bin/bash

rm results.csv

cargo build --release

for protocol in msgt nocc sgt owh owhtt
do
    for sf in 1 3
    do
        for cores in 1 5 10 20 30 40
        do
            ./target/release/spag -p $protocol -s $sf -c $cores -t $1;
        done
    done
done
