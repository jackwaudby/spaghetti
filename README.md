# Spaghetti: Yet Another Concurrency Control Evaluation Framework

Set configuration in `Settings.toml`.
```
#build
cargo build

#run server
./target/debug/spag-server

#run client
./target/debug/spag-client
```

## Framework Design ##

+ Communication between client and server is implemented using the `tokio` sync I/O crate.
+ Command line parsing is managed using the `clap` crate.
+ Configuration is managed using the `config` crate.
+ Logging use the `trace` crate.

## Concurrency Control Protocols ##

## Workloads ##

### Telecommunication Application Transaction Processing (TATP) Benchmark ###

+ Tables (0/4)
+ Transactions (0/7)

### TPC Benchmark C  ###

+ Tables (0/?)
+ Transactions (0/5)
