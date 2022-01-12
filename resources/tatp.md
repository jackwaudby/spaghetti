# TATP #

## Tables ##

| SF | `Subscribers` | `AccessInfo` | `SpecialFacility` | `CallForwarding` |
|----|---------------|--------------|-------------------|------------------|
| 0  | 10            | 10           | 10                |                  |
| 1  | 100K          | 100          | 100               |                  |
| 2  | 200K          | 1000         | 1000              |                  |

Schema:
- `Subscribers(s_id; sub_nbr; bit_1; msc_location; vlr_location)`
- `AccessInfo(id u64, balance f64)`
- `SpecialFacility(id u64, balance f64)`
- `CallForwarding(id u64, balance f64)`

Key points:
- Create sub tables with 5x space in table.
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

- `GetSubscriberData`
- `GetNewDestination`
- `GetAccessData`
- `UpdateSubscriberData`
- `UpdateLocationData`
