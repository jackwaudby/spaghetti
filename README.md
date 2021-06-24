![Git Hub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)
![Codecov](https://img.shields.io/codecov/c/github/jackwaudby/spaghetti?logo=codecov)

# Spaghetti: A Concurrency Control Evaluation Framework

This framework is designed to run on many-core machines. 
Configuration is set in `Settings.toml`.
```
#build
cargo build --release

#run
./target/release/spag -s 1 -p sgt -t 10000 -c 1
```

## ACID Test Suite

To experimentally verify the correctness of the implementations the [LDCB ACID Test Suite](http://mit.bme.hu/~szarnyas/ldbc/ldbc-acid-tpctc2020-camera-ready.pdf) was used. 
The default setting is to run each test for `10000` transactions per core, using all cores available on the test system.
Configuration is set in `./tests/Acid.toml`.
```
#run
cargo test acid -- --test-threads=1
```

| Anomaly | SGT                  | WH                   | OWH                  | 2PL                  | NOCC                 |
|---------|----------------------|----------------------|----------------------|----------------------|----------------------| 
|   G0    |TODO                  |TODO                  |TODO                  |TODO                  |TODO                  |
|   G1a   |:x:                   |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    | 
|   G1b   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|
|   G1c   |TODO                  |:heavy_check_mark:    |:x:                   |:heavy_check_mark:    |:heavy_check_mark:|
|   IMP   |TODO                  |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:x:                   |
|   PMP   |:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|:large_orange_diamond:|
|   OTV   |TODO                  |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:x:                   |
|   FR    |TODO                  |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:x:                   |
|   LU    |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:heavy_check_mark:    |:x:                   |
|   WS    |:heavy_check_mark:    |:heavy_check_mark:    |:x:                   |:heavy_check_mark:    |:x:                   |

### Comments
* `G1b` cannot be implemented as `spaghetti` does not support multiple writes to the same object within a transaction. 
* `PMP` cannot be implemented as `spaghetti` does not support predicate-based operations.
