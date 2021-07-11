![build workflow](https://github.com/jackwaudby/spaghetti/actions/workflows/build.yml/badge.svg)


# Spaghetti: A Concurrency Control Evaluation Framework

This framework is designed to run on many-core machines. 
Configuration is set in `Settings.toml`.
```
# build
cargo build --release

# run
./target/release/spag -s 1 -p sgt -t 10000 -c 1

# run all
./run.sh 

# generate throughput, latency, and abort rate plots 
./plot.sh
```

## Outputs
1. Execution summary printed to console
2. `results.csv`: execution summary
3. `log/`:  transaction responses
4. `results/`: detailed execution summary containing: (i) abort breakdown and (ii) per-transaction type breakdown 

## ACID Test Suite

To experimentally verify the correctness of the implementations the [LDCB ACID Test Suite](http://mit.bme.hu/~szarnyas/ldbc/ldbc-acid-tpctc2020-camera-ready.pdf) was used. 
The default setting is to run each test for `10000` transactions per core, using all cores available on the test system.
Configuration is set in `./tests/Acid.toml`.
```
#run
cargo test acid -- --test-threads=1
```

| Anomaly | SGT                  | WH                   | OWH                  | 2PL                  | NOCC*                | OWHTT                | 
|---------|----------------------|----------------------|----------------------|----------------------|----------------------|----------------------| 
|   G0    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:x:                   |:white_check_mark:   |
|   G1a   |:x:                   |:white_check_mark:    |:white_check_mark:    |:x:                   |:x:                   |:white_check_mark:    | 
|   G1b   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|
|   G1c   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:x:                   |:white_check_mark:    |
|   IMP   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   PMP   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:    |
|   OTV   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   FR    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   LU    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   WS    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |

### Comments
* `G1b` cannot be implemented as `spaghetti` does not support multiple writes to the same object within a transaction. 
* `PMP` cannot be implemented as `spaghetti` does not support predicate-based operations.
* `nocc` should fail all tests*.
