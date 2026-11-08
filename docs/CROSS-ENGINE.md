# Cross-engine checks: DuckDB and Spark

One-time comparison, 2026-11-07. Not a CI gate. The CI gate is `PerfCheck`, which compares the
columnar path with the row path in the same run.

## Setup

- Machine: Apple M1 Pro, 8 cores, 16 GB.
- DuckDB 2.0.0, in-memory, `PRAGMA threads=4`.
- Spark 4.3.0, `local[4]`, `spark.sql.shuffle.partitions=4`, both tables cached.
- Scarlet: 4 threads, 2 source partitions, batch 8192, columnar execution on.
- 1,000,000 rows. `k = row % 1024`, `v` from the `QueryBench` LCG, `sk` a deterministic
  Fisher-Yates permutation, and a 10,240-row join side with 10 rows per key.
- Warmup 3, measured 10, p50. The timed region is the action; data generation and table creation
  are outside it.

## Queries at 1M rows

Measured again after the scan-path work (2026-11-07, `main` @ `05d1faf`). DuckDB and Spark columns
are unchanged from the original run.

| Query | DuckDB | Spark | Scarlet |
| --- | ---: | ---: | ---: |
| filter + count | 0.86 ms | 60.35 ms | 2.26 ms |
| filter + sum | 0.97 ms | 51.30 ms | 1.86 ms |
| filter + groupByKey (materialises groups) | 1.46 ms | 82.62 ms | 8.61 ms |
| filter + reduceByKey (long pairs) | 1.78 ms | 82.58 ms | 5.9-8.5 ms |
| filter + join | 4.52 ms | 78.71 ms | 20.97 ms |
| filter + sort | 54.96 ms | 680.65 ms | 42.69 ms |

All six checksums match on all three engines. Against DuckDB, Scarlet is 1.9x slower on the sum,
2.6x on the count, 3.3-4.8x on the reduce, 4.6x on the join, and 0.78x on the sort.

**The groupByKey row is not the same query on the two sides.** DuckDB runs
`SELECT k, count(*) ... GROUP BY k` and never materialises the values. Scarlet builds every
group's value list and decodes all 1e6 values before `.size` runs. Measured split on the same data:
7.93 ms for the benchmark shape, 6.01 ms to build the groups without decoding values, and 3.29 ms
for a single-threaded direct count per key. The 5.9x figure should not be quoted as an engine gap;
a fair case needs a `countByKey`-style action that never builds the value arrays.

The Scarlet `reduceByKey` row uses the long-pair path; the int-pair path is faster on the same data.

## Spark at 10M rows

Spark, same settings, warmup 2, measured 5:

- filter + count: 176 ms
- filter + sum: 131 ms
- filter + groupByKey: 185 ms
- filter + reduceByKey: 177 ms
- filter + join: 235 ms
- filter + sort: 9,200 ms

The Scarlet 10M run did not complete under the harness's heap; the sort and the duplicated long
filter collection together exceeded the budget. That is a harness limit, not an engine failure,
but it means the 10M table is Spark-only. The Spark sort is the interesting number: it grows from
681 ms at 1M to 9.2 s at 10M, which is the shuffle and spill path showing up.

## Caveats

- DuckDB's table is materialized before the timer. Scarlet's action includes encoding the source
  arrays into column batches, which is part of its execution model. That is a real cost, but it is
  not the same phase DuckDB is timing.
- Spark's overhead at 1M rows is job scheduling, task serialization, and shuffle setup, not
  kernel throughput. A fair Spark comparison needs a cluster and at least 100M rows; this one is a
  sanity check, not a verdict.
- 1M rows in memory, no I/O, no compression on the Scarlet side. DuckDB's columnar storage is
  compressed; Scarlet's is not.
- One machine, one run, p50 of 10 (1M) and 5 (10M). The ratios are indicative, not a benchmark
  suite.
- The join is a 3.3M-row output at 1M rows; the sort is a 333k-row output.

## Reading

The remaining 2-5x gap to DuckDB on scans, aggregates and joins is the scalar JVM kernel plus the
source encode, not representation. Two measured spikes killed the earlier explanations: a 10-bit
packed column scans slower than a raw int array at this size (1.91 vs 0.84 ns/row), and the Panama
Vector API loses to the scalar loop on JDK 17 (4.95 vs 2.61 ns/row, and 1.48 vs 0.63 on a
compare-only predicate) because the preferred species is 128-bit and `div` is not native. What did
move the numbers was removing work: `count` and `sum` now walk the filter stack in place instead of
gathering (count 3.66 to 2.30 ms, allocation 22.7 to 8.3 MB at width 4), and long-pair
`reduceByKey` partial-reduces survivor ids instead of two chained filter copies (19.0 to 5.9 ms at
`CROSS_ROUNDS=20`). The sort win is the rank-split path doing what it was built for.

Spark is the right comparison for scale-out behavior, not for single-node kernel speed. At 1M rows
it mostly measures its own overhead; at 10M its sort already shows the shuffle cost. The next
Spark run should be on a cluster with a partitioned dataset and a measured shuffle, not a local
single-JVM run.

## Reproduce

```shell
# DuckDB side
python3 -m venv /tmp/cross-engine-venv
/tmp/cross-engine-venv/bin/pip install duckdb pandas pyarrow
/tmp/cross-engine-venv/bin/python cross_engine_duckdb.py

# Spark side (Java 17, Python 3.12)
/opt/homebrew/bin/python3.12 -m venv /tmp/spark-venv
/tmp/spark-venv/bin/pip install pyspark==4.3.0 pandas
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
  PYSPARK_PYTHON=/tmp/spark-venv/bin/python \
  PYSPARK_DRIVER_PYTHON=/tmp/spark-venv/bin/python \
  /tmp/spark-venv/bin/python cross_engine_spark.py

# Scarlet side
sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
```

The Python harnesses live outside the repo; the Scarlet harness is
`modules/scarlet-tests/src/test/scala/com/ewoodbury/scarlet/execution/CrossEngineBench.scala`.
