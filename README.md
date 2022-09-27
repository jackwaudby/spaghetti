![build workflow](https://github.com/jackwaudby/spaghetti/actions/workflows/build.yml/badge.svg)


# Spaghetti: A Concurrency Control Evaluation Framework

This framework is designed to run on many-core machines.
Configuration is set in `Settings.toml`.
```
# build
cargo build --release

# run
./target/release/spag -s 1 -p sgt -t 10000 -c 1
```
