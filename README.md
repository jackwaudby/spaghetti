![Git Hub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)
![Codecov](https://img.shields.io/codecov/c/github/jackwaudby/spaghetti?logo=codecov)

# Spaghetti: Yet Another Concurrency Control Evaluation Framework

This framework is designed to run on many-core machines. 
Configuration is set in `Settings.toml`.
```
#build
cargo build --release

#run
./target/debug/spag -c 1 -p sgt -t 10000
```

### Misc
```
#run tests with logging
RUST_LOG=debug cargo test -- --test-threads=1 --nocapture

#run test coverage locally
docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin:0.16.0

#generate a flamegraph (linux only)
cargo flamegraph -o 30-core.svg --bin=spag -- -c 30 -p sgt -t 1000000
```
