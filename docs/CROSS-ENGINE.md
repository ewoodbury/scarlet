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
are unchanged from the original run. The `countByKey` row was added on 2026-11-08 (`main` @
`6cf4349`): DuckDB and Spark run the same `SELECT k, count(*) ... GROUP BY k` they already ran, and
the Scarlet column is `QueryBench`'s `filter-countbykey` at four threads, p50 of 40 rounds.

| Query | DuckDB | Spark | Scarlet |
| --- | ---: | ---: | ---: |
| filter + count | 0.86 ms | 60.35 ms | 2.26 ms |
| filter + sum | 0.97 ms | 51.30 ms | 1.86 ms |
| filter + countByKey | 1.46 ms | 82.62 ms | 7.05 ms |
| filter + groupByKey (materialises groups) | 1.46 ms | 82.62 ms | 8.61 ms |
| filter + reduceByKey (long pairs) | 1.78 ms | 82.58 ms | 5.9-8.5 ms |
| filter + join | 4.52 ms | 78.71 ms | 20.97 ms |
| filter + sort | 54.96 ms | 680.65 ms | 42.69 ms |

All seven checksums match on all three engines. Against DuckDB, Scarlet is 1.9x slower on the sum,
2.6x on the count, 4.8x on the count per key, 3.3-4.8x on the reduce, 4.6x on the join, and 0.78x
on the sort.

**The `groupByKey` row is not the same query on the two sides, and the `countByKey` row is.**
DuckDB runs `SELECT k, count(*) ... GROUP BY k` and never materialises the values. For the
`groupByKey` entry Scarlet builds every group's value list and decodes all 1e6 values before
`.size` runs: measured split on the same data, 7.93 ms for that shape, 6.01 ms to build the groups
without decoding, and 3.29 ms for a single-threaded direct count per key. The `countByKey` action
counts rows per key without touching the value column, which is the same work DuckDB does, so 7.05
against 1.46 ms is the like-for-like figure; the 5.9x on the materialising row should not be quoted
as an engine gap.

One asymmetry remains in that comparison: DuckDB's statement carries `ORDER BY k` over 1024 groups,
while Scarlet's checksum folds the counts in whatever order the hash finishes in. The DuckDB number
therefore includes a small sort the Scarlet number does not.

The Scarlet `reduceByKey` row uses the long-pair path; the int-pair path is faster on the same data.

## Spark at 10M rows

Spark, same settings, warmup 2, measured 5:

- filter + count: 176 ms
- filter + sum: 131 ms
- filter + groupByKey: 185 ms
- filter + reduceByKey: 177 ms
- filter + join: 235 ms
- filter + sort: 9,200 ms

The Scarlet 10M run did not complete under the old harness's heap; the sort and the duplicated long
filter collection together exceeded the budget. That is a harness limit, not an engine failure.
The Spark sort is the interesting number there: it grows from 681 ms at 1M to 9.2 s at 10M, which
is the shuffle and spill path showing up. A later Scarlet run with one query per process did
finish. Its numbers are in the next section. They do not replace the 1M table above.

## Scarlet spilled harness, 2026-11-08

`main` at `5977dfd`. Same machine, 4 threads, batch 8192, two source partitions. Warmup 3, measured
10, p50. Spill was on for this harness only, with the 64 MiB budget. Each query had its own
process and a 4 GB heap. Two processes ran at once, so a neighbor was on the machine, except
`groupByKey` at 10M rows, which ran alone. `encode_ms` is the encode before warmup. It is not part
of the p50. The p50 still includes the scan encode. All 48 runs published at least one column
batch and exited 0. That `groupByKey` run wrote no spill file because `groupByKey` did not
spill yet. The estimate added in this change charges every input row, so a 1024-key fit does
not describe it.

`encode_ms` is outside the p50 column. A null value is a null value, at index 1 mod 4096, so the
key filter keeps it. One filter is the key filter only.

With one filter, the count, `groupByKey`, and join checksums matched the run with no nulls. The
sum, `reduceByKey`, and sort checksums did not. With two filters, every checksum changed. Those
null runs used the unboxing int-pair scan. A matching count, `groupByKey`, or join checksum does
not show that the null was kept.

### 1M rows, no nulls

| Query | 1 filter p50 | 1 filter encode | 2 filters p50 | 2 filters encode |
| --- | ---: | ---: | ---: | ---: |
| filter + count | 3.14 ms | 7.45 ms | 7.79 ms | 7.83 ms |
| filter + sum | 4.38 ms | 9.35 ms | 6.28 ms | 10.13 ms |
| filter + groupByKey | 24.92 ms | 21.79 ms | 18.48 ms | 18.64 ms |
| filter + reduceByKey | 30.08 ms | 10.08 ms | 26.63 ms | 9.35 ms |
| filter + join | 43.09 ms | 19.63 ms | 38.11 ms | 7.77 ms |
| filter + sort | 86.55 ms | 8.50 ms | 50.97 ms | 11.70 ms |

### 1M rows, null values

| Query | 1 filter p50 | 1 filter encode | 2 filters p50 | 2 filters encode |
| --- | ---: | ---: | ---: | ---: |
| filter + count | 10.03 ms | 74.71 ms | 7.97 ms | 34.21 ms |
| filter + sum | 14.54 ms | 36.54 ms | 9.36 ms | 52.20 ms |
| filter + groupByKey | 23.85 ms | 40.67 ms | 17.05 ms | 53.41 ms |
| filter + reduceByKey | 60.68 ms | 37.37 ms | 26.61 ms | 41.46 ms |
| filter + join | 71.51 ms | 42.34 ms | 29.62 ms | 78.77 ms |
| filter + sort | 99.18 ms | 52.86 ms | 68.41 ms | 55.36 ms |

### 10M rows, no nulls

| Query | 1 filter p50 | 1 filter encode | 2 filters p50 | 2 filters encode |
| --- | ---: | ---: | ---: | ---: |
| filter + count | 36.91 ms | 14.92 ms | 24.53 ms | 27.20 ms |
| filter + sum | 27.58 ms | 71.74 ms | 24.29 ms | 15.20 ms |
| filter + groupByKey | 71.92 ms | 11.98 ms | 63.21 ms | 17.36 ms |
| filter + reduceByKey | 266.11 ms | 39.23 ms | 137.12 ms | 25.99 ms |
| filter + join | 623.41 ms | 25.82 ms | 261.01 ms | 21.61 ms |
| filter + sort | 2,067.88 ms | 18.87 ms | 591.19 ms | 23.05 ms |

### 10M rows, null values

| Query | 1 filter p50 | 1 filter encode | 2 filters p50 | 2 filters encode |
| --- | ---: | ---: | ---: | ---: |
| filter + count | 80.07 ms | 109.24 ms | 56.45 ms | 126.26 ms |
| filter + sum | 232.41 ms | 245.92 ms | 149.52 ms | 224.12 ms |
| filter + groupByKey | 108.48 ms | 229.51 ms | 86.43 ms | 123.56 ms |
| filter + reduceByKey | 301.94 ms | 424.39 ms | 113.85 ms | 318.59 ms |
| filter + join | 2,570.14 ms | 310.82 ms | 621.08 ms | 224.36 ms |
| filter + sort | 2,155.56 ms | 413.04 ms | 765.98 ms | 274.90 ms |

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
`CROSS_ROUNDS=20`), and `countByKey` counts per key without the value arrays `groupByKey` builds
(7.05 against 10.35 ms at four threads, 23.6 against 47.5 MB). The sort win is the rank-split path
doing what it was built for.

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

# Scarlet side: one query per process, 4 GB heap
CROSS_ROWS=10000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_QUERY=filter-count \
  CROSS_WARMUP=3 CROSS_ROUNDS=10 \
  sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
```

`CROSS_ROWS` defaults to 1000000, `CROSS_FILTERS` to 2 (`1` is the key filter only), and
`CROSS_NULLS` to false. `CROSS_WARMUP` defaults to 3 and `CROSS_ROUNDS` to 10. Set `CROSS_QUERY`
to one of `filter-count`, `filter-sum`, `filter-groupbykey`, `filter-reducebykey`, `filter-join`,
or `filter-sort`. Omit it to run all six in that process, dropping each query's input before the
next. `encode_ms` is reported separately and is not subtracted from the action. The action timer
still includes the scan encode. Spill is on for this harness only.

The Python harnesses live outside the repo; the Scarlet harness is
`modules/scarlet-tests/src/test/scala/com/ewoodbury/scarlet/execution/CrossEngineBench.scala`.
