```
+-----------------+
|GlobalStatistics |
+--------+--------+
         | 1 per thread
         |
+--------v------+
|LocalStatistics|
+--------+------+
         |
         |
+--------v-----------+        +----------------+   +----------------------+
|TransactionBreakdown+------->| AbortBreakdown +-->|WorkloadAbortBreakdown|
+--------+-----------+        +--------+-------+   +--------------+-------+
         |                             |                          |
         | 1 per txn in workload       |                          |
+--------v---------+          +--------v-------------+      +-----v----------+
|TransactionMetrics|          |ProtocolAbortBreakdown|      |SmallBankReasons|
+------------------+          +--------+-------------+      +----------------+
                                       |
                                  +----v-----+
                                  |SgtReasons|
                                  +----------+
```                                  
                                                                                         
