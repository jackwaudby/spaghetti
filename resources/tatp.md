# TATP #

## Tables ##

| SF | `Subscribers` | `AccessInfo` | `SpecialFacility` | `CallForwarding` |
|----|---------------|--------------|-------------------|------------------|
| 0  | 10            | 25           | 25                | 37               |
| 1  | 100K          | 250K         | 250K              | 375K             |
| 2  | 200K          | 500K         | 500K              | 750K             |

Schema:
- `Subscribers(s_id; sub_nbr; bit_1; msc_location; vlr_location)`
- `AccessInfo(s_id; ai_type; data1; data2; data3; data4)`; between 1 and 4 records per subscriber.
- `SpecialFacility(s_id; sf_type; is_active; error_cntrl; data_a; data_b)`;  between 1 and 4 records per subscriber.
- `CallForwarding(s_id; sf_type; start_time; end_time; number_x)`; between 0 and 3 records per special facility row.

Key points:
- Create sub-tables with 5x space of base table.

## Contention ##

All transactions use `nurand()` or uniform.

## Workload Mix ##

- **Isolation mix:** 20/40/40 read uncommitted/read committed/serializable.
- **Balance mix:** 35/10/35/4/16 get-subscriber-data/get-new-destination/get-access-data/update-subscriber-data/call-forwarding

## Transactions ##

- `GetSubscriberData` (100%) 1 table, 5 reads.
- `GetNewDestination` (23.9%) 2 tables, 8 reads.
- `GetAccessData` (62.5%) 1 table, 4 reads.
- `UpdateLocationData`(62.5%) 1 table, 1 write.
- `UpdateSubscriberData` (100%) 1 table, 2 writes.
