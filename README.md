![Git Hub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)
![Codecov](https://img.shields.io/codecov/c/github/jackwaudby/spaghetti?logo=codecov)

# Spaghetti: Yet Another Concurrency Control Evaluation Framework

The framework can be ran in 2 modes:
+ Client-server (WIP)
+ Embedded

## Embedded Mode

The transaction generator and server run on the same machine.
Configuration is set in `Embedded.toml`.
```
#build
cargo build

#run embedded mode
./target/debug/spag-em
```

## Client-Server Mode

A server accepts transaction requests from clients over the network.
Configuration is set in:
* `Client.toml`: per client.
* `Server.toml`: server.

```
#build
cargo build

#run server
./target/debug/spag-server

#run client(s)
./target/debug/spag-client
```

## Datagen

In either mode data can be generated on the fly before each run or loaded from `csv` files, datagen produces these `csv` files.
Configuration is set in `Generator.toml`: data generation.
 Generated data is stored in `./data/`

```
#build
cargo build

#run data gen
./target/debug/spag-gen
```

## Results

Benchmark results are stored in `./results/` and a report can be produced using:
```
cd scripts/

./generate-report.sh
```

## Misc
To run tests with log info use:
```
RUST_LOG=debug cargo test -- --test-threads=1 --nocapture
```

Test coverage can be ran locally using:
```
docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin:0.16.0
```
