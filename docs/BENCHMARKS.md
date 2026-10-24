# Benchmarks

`QueryBench` is the measurement instrument for the engine. It is a `main` in `scarlet-tests`.
`make test` does not run it, and it does not assert on timing.

## Run

```shell
sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench"
```

Stdout prints a markdown table, a short note on where the columnar p50 is slower than the row
p50, and one JSON line per row:

```text
BENCH {"query":"int-narrow","layout":"columnar","threads":1,"p50_ms":0.000,"p95_ms":0.000,"alloc_mb":0.000}
```

`layout` is `columnar` or `row`. `alloc_mb` is `null` at width 4. Progress is printed on stderr.

## Method

Every query uses two source partitions and `columnarBatchSize` 8192. Element queries use 1e6
rows. Grouped queries use 1e7 rows.

Five warmup rounds are discarded. Twenty-one rounds are measured. Percentiles are nearest-rank:
the 1-based rank is `ceil(p / 100 * n)`, with no interpolation. For 21 samples, p50 is rank 11
and p95 is rank 20, the second-largest sample.

A local loop computes each checksum before timing. One row-path execution must match that loop
before any sample is kept. After each timed action the result is compared again. The comparison
sits outside the timer. A mismatch fails the run.

`alloc_mb` is an estimate. It is the nearest-rank p50 of five `ThreadMXBean` samples of bytes
allocated on the calling thread, taken after the timed rounds, and only at width 1. The unit is
decimal megabytes (`bytes / 1e6`). At width 1 the columnar kernels run on the caller. The row
scheduler can still run its task on another thread, so a row estimate can omit that work. Width
4 omits the figure. Do not compare it across thread counts.

Widths 1 and 4 share one JVM. Each round runs both widths, and the width that runs first flips
every round. Columnar and row share that JVM, and the layout that runs first flips with the
round. This is not a fresh-JVM isolation.

## Queries

| Query | Plan | What it measures |
| --- | --- | --- |
| `int-narrow` | `Pipeline2(Scan)` | `ofInts.filter(_ > 0).map(_ * 2).sum` |
| `int-chain` | `Pipeline4(Scan)` | four `map(_ + 1)` calls, then `sum`, on `ofInts` |
| `long-chain` | `Pipeline4(Scan)` | four `map(_ + 1L)` calls, then `sum`, on `ofLongs` |
| `double` | `Filter(Scan)` | `ofDoubles.filter(_ > 0.0).sum` |
| `string` | `Filter(Scan)` | dictionary `filterStringEquals("k42")`, then `count`, on `ofStrings` |
| `boolean` | `Filter(Scan)` | `ofBools.filter(identity)`, then `count` |
| `grouped-repeat` | `GroupedSum(Scan)` | `groupedSum` on keys `row % 1024`. Sequential keys, the current best case |
| `grouped-shuffled` | `GroupedSum(Scan)` | the same key multiset per morsel, in shuffled order |

Int, long, and double inputs share one linear-congruential generator (seed `0x12345678`). Each
value is in `-1000000` until `1000000`. The long and double queries widen those ints. The double
sum is an exact integer.

Strings are `k0` through `k999`, chosen by `row % 1000`, so the column has 1000 distinct values.
The filter keeps `k42`.

Booleans are true on even rows. `filter(identity)` keeps those rows.

Grouped values are the row index. Repeating keys are `row % 1024` in that order. The batch size
is a multiple of 1024, so each full morsel repeats one key sequence and the merge hits cached
slots. Shuffled keys are a Fisher-Yates permutation of each morsel with seed `58`. A morsel is
one `columnarBatchSize` range from the start of a source partition, which is how the columnar
scan slices a partition. The key multiset of each range is unchanged. Values stay in row order,
so both grouped queries have the same checksum. The tail morsel of each partition is shorter
than a full batch and is shuffled on its own.

The harness checks the physical shape before timing, so a row-path fallback is not recorded as
a columnar result.

## Baseline

Recorded on an Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-24.

| query | layout | threads | rows | p50 ms | p95 ms | alloc mb |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| int-narrow | columnar | 1 | 1000000 | 5.470 | 42.794 | 8.318 |
| int-narrow | columnar | 4 | 1000000 | 2.294 | 17.368 | n/a |
| int-narrow | row | 1 | 1000000 | 39.782 | 56.640 | 100.021 |
| int-narrow | row | 4 | 1000000 | 39.795 | 48.338 | n/a |
| int-chain | columnar | 1 | 1000000 | 6.212 | 6.502 | 8.329 |
| int-chain | columnar | 4 | 1000000 | 2.310 | 2.731 | n/a |
| int-chain | row | 1 | 1000000 | 96.915 | 219.961 | 360.002 |
| int-chain | row | 4 | 1000000 | 97.456 | 112.881 | n/a |
| long-chain | columnar | 1 | 1000000 | 17.015 | 17.780 | 16.366 |
| long-chain | columnar | 4 | 1000000 | 5.419 | 7.047 | n/a |
| long-chain | row | 1 | 1000000 | 105.816 | 111.898 | 399.997 |
| long-chain | row | 4 | 1000000 | 105.911 | 164.999 | n/a |
| double | columnar | 1 | 1000000 | 2.131 | 2.212 | 8.128 |
| double | columnar | 4 | 1000000 | 1.547 | 1.637 | n/a |
| double | row | 1 | 1000000 | 25.368 | 25.923 | 68.018 |
| double | row | 4 | 1000000 | 25.372 | 26.672 | n/a |
| string | columnar | 1 | 1000000 | 7.872 | 28.224 | 29.827 |
| string | columnar | 4 | 1000000 | 7.663 | 23.607 | n/a |
| string | row | 1 | 1000000 | 10.372 | 10.571 | 0.076 |
| string | row | 4 | 1000000 | 10.301 | 10.474 | n/a |
| boolean | columnar | 1 | 1000000 | 5.344 | 7.348 | 20.096 |
| boolean | columnar | 4 | 1000000 | 5.082 | 5.561 | n/a |
| boolean | row | 1 | 1000000 | 25.077 | 48.008 | 32.012 |
| boolean | row | 4 | 1000000 | 25.174 | 33.885 | n/a |
| grouped-repeat | columnar | 1 | 10000000 | 44.253 | 71.441 | 99.845 |
| grouped-repeat | columnar | 4 | 10000000 | 15.765 | 34.775 | n/a |
| grouped-repeat | row | 1 | 10000000 | 139.578 | 245.664 | 1259.991 |
| grouped-repeat | row | 4 | 10000000 | 131.642 | 174.598 | n/a |
| grouped-shuffled | columnar | 1 | 10000000 | 107.455 | 127.408 | 109.877 |
| grouped-shuffled | columnar | 4 | 10000000 | 33.136 | 54.448 | n/a |
| grouped-shuffled | row | 1 | 10000000 | 140.592 | 180.596 | 1259.991 |
| grouped-shuffled | row | 4 | 10000000 | 138.394 | 166.058 | n/a |

Columnar p50 was at or below the row p50 on every query and width.

At p95, `string` is slower on the columnar path than on the row path: 28.224 ms against 10.571
ms at one thread, and 23.607 ms against 10.474 ms at four threads. Every other query has a
columnar p95 below the row p95. p95 is the second-largest of the 21 samples, so one slow round
moves it.
