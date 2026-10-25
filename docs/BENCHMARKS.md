# Benchmarks

`QueryBench` is the measurement instrument for the engine. It is a `main` in `scarlet-tests`.
`make test` does not run it, and it does not assert on timing.

## Run

```shell
sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench"
```

Stderr prints the run notes, a markdown table, and a short note on where the columnar p50
is slower than the row p50. Stdout is one JSON object per line:

```text
{"schema":"query-bench-1","query":"int-narrow","layout":"columnar","threads":1,"rows":1000000,"batch":8192,"rounds":40,"p50_ms":0.000,"p95_ms":0.000,"max_ms":0.000,"alloc_mb":0.000,"checksum":0}
```

`layout` is `columnar` or `row`. `alloc_mb` is `null` when the thread bean is unavailable.
Progress is on stderr. sbt may still prefix lines; the objects themselves have no label.

## Method

Every query uses two source partitions and `columnarBatchSize` 8192. Element queries use 1e6
rows. Grouped queries use 1e7 rows.

Five warmup rounds are discarded. Forty rounds are measured. Forty keeps an even leader split
and leaves two samples strictly above p95. Percentiles are nearest-rank: the 1-based rank is
`ceil(p / 100 * n)`, with no interpolation. For 40 samples, p50 is rank 20, p95 is rank 38, and
max is rank 40. p95 is not a stable tail. The table reports p50, p95, and max.

`runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
JIT noise. Comparisons within one run are the signal.

A local loop computes each scalar checksum before timing. One row-path execution must match that
loop before any sample is kept. After each timed action the result is compared again. The
comparison sits outside the timer. A mismatch fails the run.

Each sample installs a `CountingSink` outside the timer. A columnar sample must publish at least
one column batch. A row sample must publish none. That check is per width and per layout. A
mismatch fails the run, and no table is printed.

Grouped queries also record a per-group checksum: the sum of `(key * 31) xor groupSum` over the
1024 groups, folded in key order. The grand total is invariant if keys and values are reassigned,
so it is not this checksum. The row oracle folds the pairs that path reads. The columnar check
folds the groups `sumByKey` returns. The two checksums must match, and `grouped-shuffled` must
not match `grouped-repeat`, because the shuffle reassigns keys while the grand total stays the
same. Those checks run at every width, before warmup and again after the measured rounds, outside
the timer.

`alloc_mb` is an estimate. It is the nearest-rank p50 of five process-wide samples, taken after
the timed rounds, at both widths. A probe sums `ThreadMXBean.getThreadAllocatedBytes` across live
threads while the action runs and excludes its own thread. The unit is decimal megabytes
(`bytes / 1e6`). A worker that starts and exits between reads is missing. Unrelated JVM threads
are included. Row and columnar use the same method, so the column is comparable inside this run.

Widths 1 and 4 share one JVM. Width order and layout order flip on different bits of the round,
and each leads on 20 of the 40 measured rounds. Measure round 0 uses the order of warmup round 1,
so the two phases do not restart on the same order. One `LocalTaskScheduler` per width is created
for the run and shut down at the end. This is not a fresh-JVM isolation.

## Queries

| Query | Plan | What it measures |
| --- | --- | --- |
| `int-narrow` | `Pipeline2(Scan)` | `ofInts.filter(_ > 0).map(_ * 2).sum` |
| `int-chain` | `Pipeline4(Scan)` | four `map(_ + 1)` calls, then `sum`, on `ofInts` |
| `long-chain` | `Pipeline4(Scan)` | four `map(_ + 1L)` calls, then `sum`, on `ofLongs` |
| `double` | `Filter(Scan)` | `ofDoubles.filter(_ > 0.0).sum` |
| `string` | `Filter(Scan)` | dictionary `filterStringEquals("k42")`, then `count`, on `ofStrings` |
| `boolean` | `Filter(Scan)` | `ofBools.filter(identity)`, then `count` |
| `grouped-repeat` | `GroupedSum(Scan)` | `groupedSum` on keys `row % 1024`, in order |
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

The harness checks the physical shape before timing. It also rejects a sample whose published
batches disagree with the layout it is about to record.

## Baseline

Recorded on an Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-24. Absolute numbers carry sbt and JIT noise. The comparison
that matters is within this run.

| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| int-narrow | columnar | 1 | 1000000 | 5.490 | 5.824 | 5.919 | 8.316 |
| int-narrow | columnar | 4 | 1000000 | 2.225 | 2.385 | 2.594 | 8.333 |
| int-narrow | row | 1 | 1000000 | 34.068 | 34.813 | 50.423 | 100.045 |
| int-narrow | row | 4 | 1000000 | 33.897 | 35.037 | 45.433 | 100.043 |
| int-chain | columnar | 1 | 1000000 | 6.208 | 6.374 | 6.460 | 8.327 |
| int-chain | columnar | 4 | 1000000 | 2.248 | 2.340 | 2.434 | 8.344 |
| int-chain | row | 1 | 1000000 | 90.684 | 152.601 | 157.879 | 360.026 |
| int-chain | row | 4 | 1000000 | 90.448 | 157.640 | 160.223 | 360.024 |
| long-chain | columnar | 1 | 1000000 | 17.021 | 17.413 | 17.552 | 16.361 |
| long-chain | columnar | 4 | 1000000 | 5.397 | 5.619 | 6.024 | 16.377 |
| long-chain | row | 1 | 1000000 | 97.099 | 154.100 | 162.527 | 400.021 |
| long-chain | row | 4 | 1000000 | 97.018 | 164.784 | 168.438 | 400.019 |
| double | columnar | 1 | 1000000 | 2.114 | 2.227 | 3.013 | 8.129 |
| double | columnar | 4 | 1000000 | 1.487 | 1.648 | 1.750 | 8.131 |
| double | row | 1 | 1000000 | 23.489 | 24.395 | 51.858 | 68.041 |
| double | row | 4 | 1000000 | 23.376 | 24.104 | 51.869 | 68.039 |
| string | columnar | 1 | 1000000 | 7.721 | 7.987 | 8.231 | 29.823 |
| string | columnar | 4 | 1000000 | 7.316 | 7.639 | 58.673 | 26.563 |
| string | row | 1 | 1000000 | 10.218 | 10.432 | 10.498 | 0.098 |
| string | row | 4 | 1000000 | 10.136 | 10.309 | 10.425 | 0.097 |
| boolean | columnar | 1 | 1000000 | 5.240 | 5.343 | 23.395 | 20.092 |
| boolean | columnar | 4 | 1000000 | 4.801 | 4.992 | 5.050 | 19.044 |
| boolean | row | 1 | 1000000 | 25.210 | 36.349 | 36.920 | 32.033 |
| boolean | row | 4 | 1000000 | 25.137 | 36.026 | 37.180 | 32.033 |
| grouped-repeat | columnar | 1 | 10000000 | 18.828 | 19.627 | 29.611 | 99.668 |
| grouped-repeat | columnar | 4 | 10000000 | 8.735 | 27.229 | 31.038 | 101.381 |
| grouped-repeat | row | 1 | 10000000 | 137.152 | 184.442 | 187.443 | 1260.014 |
| grouped-repeat | row | 4 | 10000000 | 133.183 | 138.352 | 142.353 | 1260.015 |
| grouped-shuffled | columnar | 1 | 10000000 | 24.640 | 25.387 | 29.685 | 99.667 |
| grouped-shuffled | columnar | 4 | 10000000 | 10.192 | 11.089 | 31.425 | 101.852 |
| grouped-shuffled | row | 1 | 10000000 | 145.630 | 180.665 | 184.434 | 1260.015 |
| grouped-shuffled | row | 4 | 10000000 | 144.217 | 148.435 | 150.966 | 1260.014 |

Columnar p50 was at or below the row p50 on every query and width. Columnar p95 was also below
the row p95 on every query and width. Two samples sit above p95, so the tail is not a stable
ranking.

`grouped-repeat` and `grouped-shuffled` disagree on the per-group checksum
(`49999995039680` and `49999995026112`). Each checksum matches across both layouts and both
widths. The string row estimate stays near 0.1 MB. That action walks strings that already exist,
and the same process-wide probe reports about 30 MB for the columnar dictionary encode.

A grouped partial whose present keys fit the direct-index budget does not probe the hash table.
On this run, `grouped-shuffled` at one thread is 24.640 ms and `grouped-repeat` is 18.828 ms.
