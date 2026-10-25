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

The harness checks the physical shape before timing. It also rejects a sample whose published
batches disagree with the layout it is about to record.

## Baseline

Recorded on an Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-24. Absolute numbers carry sbt and JIT noise. The comparison
that matters is within this run.

| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| int-narrow | columnar | 1 | 1000000 | 5.408 | 5.524 | 5.610 | 8.319 |
| int-narrow | columnar | 4 | 1000000 | 2.219 | 2.326 | 2.349 | 8.330 |
| int-narrow | row | 1 | 1000000 | 34.936 | 35.623 | 50.823 | 100.044 |
| int-narrow | row | 4 | 1000000 | 34.982 | 61.353 | 63.246 | 100.043 |
| int-chain | columnar | 1 | 1000000 | 6.204 | 6.393 | 6.405 | 8.329 |
| int-chain | columnar | 4 | 1000000 | 2.281 | 2.402 | 2.475 | 8.340 |
| int-chain | row | 1 | 1000000 | 92.994 | 162.717 | 165.168 | 360.026 |
| int-chain | row | 4 | 1000000 | 92.950 | 159.582 | 161.007 | 360.024 |
| long-chain | columnar | 1 | 1000000 | 16.962 | 18.164 | 33.770 | 16.363 |
| long-chain | columnar | 4 | 1000000 | 5.433 | 5.638 | 5.898 | 16.373 |
| long-chain | row | 1 | 1000000 | 99.921 | 167.118 | 170.266 | 400.034 |
| long-chain | row | 4 | 1000000 | 100.020 | 168.361 | 170.616 | 400.032 |
| double | columnar | 1 | 1000000 | 2.108 | 2.245 | 2.487 | 8.129 |
| double | columnar | 4 | 1000000 | 1.505 | 1.599 | 1.731 | 8.131 |
| double | row | 1 | 1000000 | 22.355 | 23.348 | 40.085 | 68.041 |
| double | row | 4 | 1000000 | 22.373 | 23.630 | 38.687 | 68.039 |
| string | columnar | 1 | 1000000 | 7.558 | 7.899 | 8.097 | 29.827 |
| string | columnar | 4 | 1000000 | 7.161 | 7.521 | 7.539 | 25.641 |
| string | row | 1 | 1000000 | 10.363 | 10.698 | 10.785 | 0.098 |
| string | row | 4 | 1000000 | 10.276 | 10.520 | 10.722 | 0.097 |
| boolean | columnar | 1 | 1000000 | 5.246 | 5.474 | 5.487 | 20.096 |
| boolean | columnar | 4 | 1000000 | 4.794 | 5.027 | 5.194 | 18.910 |
| boolean | row | 1 | 1000000 | 25.490 | 36.643 | 37.321 | 32.033 |
| boolean | row | 4 | 1000000 | 25.250 | 34.776 | 49.661 | 32.033 |
| grouped-repeat | columnar | 1 | 10000000 | 43.782 | 48.145 | 56.476 | 99.914 |
| grouped-repeat | columnar | 4 | 10000000 | 15.025 | 16.046 | 44.128 | 99.976 |
| grouped-repeat | row | 1 | 10000000 | 127.657 | 171.374 | 175.887 | 1259.992 |
| grouped-repeat | row | 4 | 10000000 | 126.375 | 130.669 | 133.810 | 1260.014 |
| grouped-shuffled | columnar | 1 | 10000000 | 108.615 | 160.235 | 163.506 | 109.946 |
| grouped-shuffled | columnar | 4 | 10000000 | 33.193 | 34.464 | 34.699 | 109.538 |
| grouped-shuffled | row | 1 | 10000000 | 136.749 | 153.944 | 180.577 | 1260.014 |
| grouped-shuffled | row | 4 | 10000000 | 121.289 | 142.368 | 142.721 | 1260.004 |

Columnar p50 was at or below the row p50 on every query and width.

`grouped-repeat` and `grouped-shuffled` disagree on the per-group checksum
(`49999995039680` and `49999995026112`). Each checksum matches across both layouts and both
widths. The string row estimate stays near 0.1 MB. That action walks strings that already exist,
and the same process-wide probe reports about 30 MB for the columnar dictionary encode.

At p95, `grouped-shuffled` at one thread is slower on the columnar path (160.235 ms) than on the
row path (153.944 ms). The columnar max is 163.506 ms and the row max is 180.577 ms. Two samples
sit above p95, so this is not a stable tail ranking. Every other query has a columnar p95 below
the row p95.
