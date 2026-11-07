# Cross-engine check: DuckDB

One-time comparison, 2026-11-07. Not a CI gate. The CI gate is `PerfCheck`, which compares the
columnar path with the row path in the same run.

## Setup

- Machine: Apple M1 Pro, 8 cores, 16 GB.
- DuckDB 2.0.0, in-memory, `PRAGMA threads=4`.
- Scarlet: 4 threads, 2 source partitions, batch 8192, columnar execution on.
- 1,000,000 rows. `k = row % 1024`, `v` from the `QueryBench` LCG, `sk` a deterministic
  Fisher-Yates permutation, and a 10,240-row join side with 10 rows per key.
- Warmup 3, measured 10, p50. The timed region is the action; data generation and table creation
  are outside it.

## Queries

| Query | DuckDB | Scarlet |
| --- | ---: | ---: |
| filter + count | 0.86 ms | 3.78 ms |
| filter + sum | 0.97 ms | 6.35 ms |
| filter + groupByKey | 1.46 ms | 7.79 ms |
| filter + reduceByKey | 1.78 ms | 7.64 ms |
| filter + join | 4.52 ms | 22.16 ms |
| filter + sort | 54.96 ms | 40.44 ms |

All six checksums match. The sort is the one case where Scarlet is faster (0.74x DuckDB's time);
the scan, aggregate, and join cases are 4.3-6.6x slower.

## Caveats

- DuckDB's table is materialized before the timer. Scarlet's action includes encoding the source
  arrays into column batches, which is part of its execution model. That is a real cost, but it is
  not the same phase DuckDB is timing.
- 1M rows in memory, no I/O, no compression on the Scarlet side. DuckDB's columnar storage is
  compressed; Scarlet's is not.
- One machine, one run, p50 of 10. The ratios are indicative, not a benchmark suite.
- The join is a 3.3M-row output; the sort is a 333k-row output.

## Reading

The 4-6x gap on scans and aggregates is the expected cost of JVM execution, source encoding, and
no compression or SIMD. The sort win is the rank-split path doing what it was built for. The
useful target is the scan/aggregate gap: compression and a cheaper source encode are the two
levers that would move it, and both are already on the backlog.

## Reproduce

```shell
# DuckDB side
python3 -m venv /tmp/cross-engine-venv
/tmp/cross-engine-venv/bin/pip install duckdb pandas pyarrow
/tmp/cross-engine-venv/bin/python cross_engine_duckdb.py

# Scarlet side
sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
```

The Python harness lives outside the repo; the Scarlet harness is
`modules/scarlet-tests/src/test/scala/com/ewoodbury/scarlet/execution/CrossEngineBench.scala`.
