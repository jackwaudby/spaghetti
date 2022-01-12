# SmallBank #

## Tables ##

| SF | `Accounts` | `Savings` | `Checking` |
|----|------------|-----------|------------|
| 0  | 10         | 10        | 10         |
| 1  | 100        | 100       | 100        |
| 2  | 1000       | 1000      | 1000       |
| 3  | 10000      | 10000     | 10000      |
| 4  | 100000     | 100000    | 100000     |
| 5  | 100000     | 1000000   | 1000000    |

Schema:
- `Accounts(id u64)`
- `Savings(id u64, balance f64)`
- `Checking(id u64, balance f64)`

Key points:
- id starts at 0.
- Initial balance between 10K and 50K.

## Contention ##

All transactions use `get_name()` except `SendPayment` and `Amalgamate` which
uses `get_names()`.
If the number of accounts is less than or equal to 100 pick a random account.
Else, 25% of the time pick from the hotspot (0-99), 75% pick from the non-spot.

## Workload Mix ##

- **Isolation mix:** 20/40/40 read uncommitted/read committed/serializable.
- **Uniform mix:** 15/15/15/15/15/25 balance/deposit-checking/transact-saving/amalgamate/write-check/send-payment.
- **Balance mix:** 60/8/8/8/8/8 balance/deposit-checking/transact-saving/amalgamate/write-check/send-payment.

## Transactions ##

- `Balance`: 3 tables, 3 reads.
- `DepositChecking`: 2 tables, 2 reads, 1 write.
- `TransactSaving`: 2 tables, 1 read, 1 write, aborts if insufficient funds.
- `Amalgamate`:  3 tables, 5 reads, 3 writes.
- `WriteCheck`:  3 tables, 3 reads, 1 write.
- `SendPayment`: 2 tables, 4 reads, 2 writes, aborts if insufficient funds.
