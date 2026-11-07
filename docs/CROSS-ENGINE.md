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

| Query | DuckDB | Spark | Scarlet |
| --- | ---: | ---: | ---: |
| filter + count | 0.86 ms | 60.35 ms | 3.78 ms |
| filter + sum | 0.97 ms | 51.30 ms | 6.35 ms |
| filter + groupByKey | 1.46 ms | 82.62 ms | 7.79 ms |
| filter + reduceByKey | 1.78 ms | 82.58 ms | 14.52 ms |
| filter + join | 4.52 ms | 78.71 ms | 22.16 ms |
| filter + sort | 54.96 ms | 680.65 ms | 40.44 ms |

All six checksums match on all three engines. Against DuckDB, Scarlet is 4.3-8.2x slower on the
scan, aggregate, and join cases and 0.74x on the sort. Against Spark at this size, Scarlet is
3.6-16.8x faster on every case, but that is not a scale-out result: Spark local mode at 1M rows is
dominated by job and scheduler overhead. The Scarlet `reduceByKey` row uses the long-pair path;
the int-pair path is 7.6 ms on the same data.

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

The 4-6x gap to DuckDB on scans and aggregates is the expected cost of JVM execution, source
encoding, and no compression or SIMD. The sort win is the rank-split path doing what it was built
for. The useful target is the scan/aggregate gap: compression and a cheaper source encode are the
two levers that would move it, and both are already on the backlog.

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
