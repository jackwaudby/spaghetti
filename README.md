![Git Hub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)

# Spaghetti: A Concurrency Control Evaluation Framework

This framework is designed to run on many-core machines. 
Configuration is set in `Settings.toml`.
```
# build
cargo build --release

# run
./target/release/spag -s 1 -p sgt -t 10000 -c 1

# run all
# - runs each protocol, for each workload, for each scale factor, for 1 to 60 cores
# - produces results.csv 
./run.sh 

# generate throughput, latency, and abort rate plots 
# - expects: results.csv and durner_sgt.csv
# - stored in graphics/
./plot.sh
```

## Outputs
1. Execution summary printed to console
2. `results.csv`: execution summary
3. `log/<workload>/`:  transaction responses on a per-thread basis
4. `results/<workload>/<protocol>-<scale_factor>.json`: detailed execution summary containing: (i) abort breakdown and (ii) per-transaction type breakdown 

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
|   G0    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:   |
|   G1a   |:x:                   |:white_check_mark:    |:white_check_mark:    |:x:                   |:x:                   |:white_check_mark:    | 
|   G1b   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|
|   G1c   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:x:                   |:white_check_mark:    |
|   IMP   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   PMP   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:    |
|   OTV   |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   FR    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   LU    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:x:                   |:white_check_mark:    |
|   WS    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |:white_check_mark:    |

### Comments
* `G1b` cannot be implemented as `spaghetti` does not support multiple writes to the same object within a transaction. 
* `PMP` cannot be implemented as `spaghetti` does not support predicate-based operations.
* `nocc` should fail all tests*.
