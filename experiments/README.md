# Experiments 

## Protocols 

- SGT with reduced DFS
- MSGT;
    - reduced DFS
    - restricted DFS
    - relevant DFS

## SmallBank 

Contention (`scale_factor`):
1. `high` contention (100 accounts) (`sf=1`)
2. `mid` contention (1000 accounts) (`sf=2`)
3. `low` contention (10000 accounts) (`sf=3`)

Transaction Mixes (`use_balance_mix`):
1. `uniform`: all 15% except 25% send payment 
2. `balance`: all 8% except 60% balance 

Isolation Mixes (`isolation_mix`):
1. `high`: 100% serializable 
2. `mid`: 19% read uncommitted, 60% read committed, 21% serializable
3. `low`: 18% read uncommitted, 70% read committed, 2% serializable

Misc:
- NFN runs all 3 with uniform mix
- NFN reports high (thpt/abr/lat), mid (thpt/abr), lat (thpr)

## TATP 

Base Table Size (`scale_factor`):
1. 100 entries (`sf=0`)
2. 100,000 entries  (`sf=1`)

Misc:
- Non-uniform access (`nurand`)
- All transactions execute at read committed as per the specification.
- NFN uses 100 entries in the base table. TATP specification uses 100000 
- NFN reports thpt/abr

## YCSB 

Parameters:
- `scale_factor`: table size;
    - `sf=1`: 100 entries
    - `sf=2`: 100K entries 
    - `sf=3`: 10M entries     
- `theta`: contention (0.0-0.9)
- `update_rate`: update ops per transaction (0.0-1.0)
- `serializable_rate`: proportion of serializable transactions, 10% of the remainder are read uncommitted
- `queries`: queries per transaction

Scalability: `theta=0.8`, `update_rate=0.5`, `serializable_rate=0.2`, vary `cores=1-40`
Contention: `cores=40`, `update_rate=0.5`, `serializable_rate=0.2`, vary `theta=0.0-0.9`
Isolation: `cores=40`, `update_rate=0.5`, `theta=0.8`, vary `serializable_rate=0.0-1.0`

Misc:
- Use 10 queries/txn
- NFN uses:
    - Standard workload A. 16 queries/txn, U=0.5. 10M records, varies theta 0 to 0.9. Reports thpt/abr.
    - Standard workload B. 16 queries/txn, U=0.05. 10M records, varies theta 0 to 0.9. Reports thpt.

## Space, Time & Misc

1. For each workload, average CPU cycles for cycle checking and transaction
2. Overhead of maintaining accesses, conflict detection, cycle tests, aborts, and live-lock handling. Txn/s of nocc vs SGT
3. Average transaction memory consumption
4. Types of cycles found 
5. Average cycle length
6. Conflicts detected 

## TODO 
- Track conflicts detected 
- Track average cycle length 
- Track type of cycles found 
- Nocc vs SGT vs MSGT experiment

