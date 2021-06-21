#!/bin/bash

cargo build --release

for protocol in nocc sgt owh
do
    for sf in 1 3
    do
        for cores in 1 5 10 20 30 40 50 60
        do
            ./target/release/spag -p $protocol -s $sf -c $cores -t 1000000;
        done
    done
done
