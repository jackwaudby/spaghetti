![GitHub Workflow Status](https://img.shields.io/github/workflow/status/jackwaudby/spaghetti/Rust?logo=Github)
![Codecov](https://img.shields.io/codecov/c/github/jackwaudby/spaghetti?logo=codecov)

# Spaghetti: Yet Another Concurrency Control Evaluation Framework

Set configuration in `Settings.toml`.
```
#build
cargo build

#run server
./target/debug/spag-server

#run client
./target/debug/spag-client

# Test coverage
docker run --security-opt seccomp=unconfined -v "${PWD}:/volume" xd009642/tarpaulin:0.16.0
```
