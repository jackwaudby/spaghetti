![Git Hub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)
![Codecov](https://img.shields.io/codecov/c/github/jackwaudby/spaghetti?logo=codecov)

# Spaghetti: Yet Another Concurrency Control Evaluation Framework

Set configuration:
* `Generator.toml`: data generation.
* `Client.toml`: per client.
* `Server.toml`: server.

```
#build
cargo build

#run data gen
./target/debug/spag-gen

#run server
./target/debug/spag-server

#run client
./target/debug/spag-client

#run embedded mode
./target/debug/spag-em

# Test coverage
docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin:0.16.0
```

* Generated data is stored in `./data/`
* Runtime logging and transaction response are stored in `./log/`
* Benchmark statistics are stored in `./results/`
