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
| `grouped-sparse` | `GroupedSum(Scan)` | 1024 groups spread over `0` until `300000` |
| `grouped-shifting` | `GroupedSum(Scan)` | that spread, plus 7 on every odd morsel |

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

`grouped-sparse` assigns 1024 slots across `0` until `300000`, reversed inside each morsel so the
first key is `300000`. `grouped-shifting` adds 7 on every odd morsel, so the minimum moves and
the span stays wide. Both fail the density test. The harness compares one partial with first-seen
order before timing and fails the run if that partial is sorted. A repeating or shuffled morsel
of keys `0` until `1023` has span 1024, which is inside `4 * 8192`, so those queries stay on the
direct index.

The harness checks the physical shape before timing. It also rejects a sample whose published
batches disagree with the layout it is about to record.

## Baseline

Recorded on an Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-24. Absolute numbers carry sbt and JIT noise. The comparison
that matters is within this run.

| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| int-narrow | columnar | 1 | 1000000 | 5.666 | 7.198 | 9.862 | 8.344 |
| int-narrow | columnar | 4 | 1000000 | 2.356 | 17.202 | 22.863 | 8.355 |
| int-narrow | row | 1 | 1000000 | 34.964 | 62.328 | 64.714 | 100.044 |
| int-narrow | row | 4 | 1000000 | 34.618 | 48.408 | 53.598 | 100.043 |
| int-chain | columnar | 1 | 1000000 | 6.269 | 6.533 | 6.588 | 8.329 |
| int-chain | columnar | 4 | 1000000 | 2.431 | 2.856 | 7.050 | 8.341 |
| int-chain | row | 1 | 1000000 | 93.852 | 167.971 | 184.356 | 360.026 |
| int-chain | row | 4 | 1000000 | 93.422 | 159.615 | 212.559 | 360.024 |
| long-chain | columnar | 1 | 1000000 | 17.317 | 17.615 | 21.364 | 16.363 |
| long-chain | columnar | 4 | 1000000 | 6.136 | 6.461 | 8.745 | 16.373 |
| long-chain | row | 1 | 1000000 | 100.725 | 157.549 | 229.151 | 400.021 |
| long-chain | row | 4 | 1000000 | 100.721 | 227.131 | 229.918 | 400.019 |
| double | columnar | 1 | 1000000 | 2.173 | 2.242 | 2.316 | 8.129 |
| double | columnar | 4 | 1000000 | 1.551 | 1.693 | 5.667 | 8.131 |
| double | row | 1 | 1000000 | 23.701 | 50.436 | 94.857 | 68.041 |
| double | row | 4 | 1000000 | 23.718 | 51.169 | 52.515 | 68.039 |
| string | columnar | 1 | 1000000 | 7.885 | 8.106 | 12.089 | 29.827 |
| string | columnar | 4 | 1000000 | 7.378 | 8.048 | 11.490 | 26.848 |
| string | row | 1 | 1000000 | 10.542 | 10.822 | 10.911 | 0.098 |
| string | row | 4 | 1000000 | 10.504 | 10.759 | 11.026 | 0.097 |
| boolean | columnar | 1 | 1000000 | 5.417 | 5.562 | 6.072 | 20.096 |
| boolean | columnar | 4 | 1000000 | 4.979 | 6.171 | 8.747 | 18.948 |
| boolean | row | 1 | 1000000 | 25.575 | 35.644 | 71.687 | 32.033 |
| boolean | row | 4 | 1000000 | 24.898 | 34.834 | 38.237 | 32.032 |
| grouped-repeat | columnar | 1 | 10000000 | 20.865 | 25.098 | 74.387 | 99.186 |
| grouped-repeat | columnar | 4 | 10000000 | 9.692 | 14.761 | 26.961 | 98.589 |
| grouped-repeat | row | 1 | 10000000 | 144.679 | 186.862 | 243.628 | 1260.042 |
| grouped-repeat | row | 4 | 10000000 | 145.845 | 164.991 | 187.661 | 1260.043 |
| grouped-shuffled | columnar | 1 | 10000000 | 27.160 | 31.355 | 134.015 | 99.185 |
| grouped-shuffled | columnar | 4 | 10000000 | 11.297 | 30.637 | 66.788 | 98.634 |
| grouped-shuffled | row | 1 | 10000000 | 123.545 | 178.675 | 199.703 | 1260.018 |
| grouped-shuffled | row | 4 | 10000000 | 123.002 | 165.653 | 185.803 | 1260.018 |
| grouped-sparse | columnar | 1 | 10000000 | 59.641 | 61.127 | 115.340 | 99.909 |
| grouped-sparse | columnar | 4 | 10000000 | 20.313 | 54.016 | 124.731 | 99.642 |
| grouped-sparse | row | 1 | 10000000 | 111.155 | 149.036 | 173.367 | 1279.862 |
| grouped-sparse | row | 4 | 10000000 | 111.286 | 148.374 | 165.135 | 1279.836 |
| grouped-shifting | columnar | 1 | 10000000 | 66.659 | 68.774 | 72.362 | 110.169 |
| grouped-shifting | columnar | 4 | 10000000 | 24.278 | 63.483 | 91.360 | 109.088 |
| grouped-shifting | row | 1 | 10000000 | 111.349 | 168.691 | 231.926 | 1279.862 |
| grouped-shifting | row | 4 | 10000000 | 111.323 | 168.777 | 180.535 | 1279.863 |

Columnar p50 was at or below the row p50 on every query and width. Two samples sit above p95, so
the tail is not a stable ranking. The non-grouped rows moved with this run because the sparse
queries share the JVM.

`grouped-repeat` and `grouped-shuffled` disagree on the per-group checksum
(`49999995039680` and `49999995026112`). `grouped-sparse` is `49999624077338` and
`grouped-shifting` is `50000486424568`. Each checksum matches across both layouts and both
widths. The string row estimate stays near 0.1 MB. That action walks strings that already exist,
and the same process-wide probe reports about 30 MB for the columnar dictionary encode.

A slot is 17 bytes: the sum, the flag, the touched-key list, and the zero-origin key cache. The
budget is 4 MiB across the workers, so one worker caps at 246723 slots and four workers cap at
61680 slots, 4194240 bytes. A 65536-slot prefix does not fit at four workers. The table is the
observed window, and only when that span is also at most four times the morsel length. On this
run, columnar `grouped-repeat` is 20.865 ms at one thread and 9.692 ms at four, where p95 is
14.761 ms. `grouped-shuffled` is 27.160 ms at one thread. The row engine is unchanged. Row p50
is 144.679 ms and 145.845 ms for `grouped-repeat`, and 123.545 ms and 123.002 ms for
`grouped-shuffled`.

The stage study on the same machine, in a separate process, times the partial alone. Repeating
keys: one thread p50 16.207 ms, p95 16.492, max 17.802; four threads p50 4.783, p95 6.946, max
8.288. Shuffled keys: one thread p50 23.113, p95 26.148, max 26.675; four threads p50 6.453,
p95 11.415, max 11.926.
