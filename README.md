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

### Storage Layer ### 
Spaghetti uses a simple in-memory storage layer to store data. The smallest unit is a `Field` which stores a `Option<Data>`; currently Spaghetti supports 3 data types, `i64`, `f64`, and `String`. `Field`s are stored in `Row`s, which also contain a primary key, row id, and pointer to a `Table`. A `Table` contains the schema in a `Catalog`, along with the names of primary and secondary indexes. `Table`s **do not own** `Row`s, they merely contain the information about them which is used for access, e.g., the next avaliable row id. `Row`s are owned by the primary index. Spaghetti uses the concurrent hashmap provided by the `chashmap` crate, with `Row`s accessed by their primary key.

Given a table name, the list of tables are consulted in order to get the name of the index on this table. The index is then retrieved from the list of indexes. After the primary key for the desired row is calculated it can be retrieved from the index. Retrieving a specific field from the row uses information stored in its corresponding catalog to identify the index in the row from the desired column. 

## Concurrency Control Protocols ##

## Workloads ##

### Telecommunication Application Transaction Processing (TATP) Benchmark ###


+ Table loaders (4/4)
+ Parameter generation (7/7)
+ Stored procedures (0/7)

### TPC Benchmark C  ###

+ Tables (3/?)
+ Transactions (0/5)
