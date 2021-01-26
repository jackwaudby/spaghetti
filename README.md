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
```

## Concurrency Control Protocols ##

- [ ] 2PL 
- [ ] SGT 
- [ ] Hit-list 
- [ ] Mixed SGT 

## Workloads ##

### Telecommunication Application Transaction Processing (TATP) Benchmark ###

+ Table loaders (4/4)
+ Parameter generation (7/7)
+ Transaction profiles (7/7)
+ Stored procedures (0/7)

### TPC Benchmark C (TPC-C) ###

+ Table loaders (3/9)
+ Parameter generation (0/5)
+ Transaction profiles (0/5)
+ Stored procedures (0/5)
