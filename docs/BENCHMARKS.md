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
| `int-sort-key` | `SortByIntKey(Scan)` | `ofIntPairs` sorted by key, then a checksum of the key/value sequence |
| `long-sort-key` | `SortByLongKey(Scan)` | the same pairs widened to long, sorted by key, then the same checksum |
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

Sort keys are a Fisher-Yates permutation of `0` until `1000000`, seeded by `0x12345678` through
`java.util.Random`. Values are the shared generator. Long keys and values are those ints widened.
The keys are unique, so the sorted order is one sequence. The checksum is a rolling mix,
`hash * 31 + key` then `hash * 31 + value`, over the collected pairs. A local stable sort computes
the expected checksum before timing. The comparison is outside the timer. The timed action includes
the plan, the rank split, the decode, and that mix. There is no timing assertion.

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

## Sort

Recorded on the same Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-29. The preamble printed
`java=OpenJDK 64-Bit Server VM 17.0.16+0`. Absolute numbers carry sbt and JIT noise. The
comparison that matters is within this run. The 2026-10-24 table above is unchanged. These are
the only new queries.

| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| int-sort-key | columnar | 1 | 1000000 | 177.365 | 265.825 | 275.893 | 133.394 |
| int-sort-key | columnar | 4 | 1000000 | 114.129 | 195.572 | 231.478 | 133.396 |
| int-sort-key | row | 1 | 1000000 | 866.004 | 1066.000 | 1127.953 | 455.994 |
| int-sort-key | row | 4 | 1000000 | 676.104 | 774.692 | 832.530 | 455.937 |
| long-sort-key | columnar | 1 | 1000000 | 190.047 | 268.126 | 301.522 | 173.392 |
| long-sort-key | columnar | 4 | 1000000 | 124.295 | 197.910 | 228.596 | 173.393 |
| long-sort-key | row | 1 | 1000000 | 977.116 | 1126.654 | 1151.228 | 503.998 |
| long-sort-key | row | 4 | 1000000 | 652.861 | 790.086 | 907.320 | 503.931 |

Both queries share checksum `-2192214190940607341`. The long pairs are the int pairs widened, and
the keys are unique, so the sorted sequences match. Columnar p50 was below the row p50 at both
widths. Two samples sit above p95. The harness compared each checksum to a local stable sort
outside the timer. The run rejected nothing.

An earlier study reported 6-11x for a prototype of the sort kernel. This table times the
production path. The timed action includes the plan and the rank split, then the decode and the
checksum mix. A long-pair sort spills when `spillEnabled` is on and the retained estimate is over
`spillBudgetBytes`. One input partition is still sorted in memory when spill is on. This run
leaves spill off, so the timed path is the in-memory rank split.

## Spill

`SpillBench` times grouped sum and sort-merge with spill off, with spill on at the default
budget, and with spill on at a 64 KiB stress budget. It is a `main` in `scarlet-tests`.
`make test` does not run it, and it does not assert on timing.

### Run

```shell
sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.SpillBench"
```

Stderr prints the run notes and a markdown table. Stdout is one JSON object per line. The object
below is the field shape. Its numbers are placeholders, not a measurement:

```text
{"schema":"spill-bench-2","case":"grouped-sum","spill":false,"mode":"off","budget_bytes":67108864,"threads":1,"rows":1000000,"rounds":5,"p50_ms":0.000,"p95_ms":0.000,"max_ms":0.000,"files_written":0,"merge_passes":0,"checksum":0}
```

`spill` is `true` or `false`. `mode` is `off`, `default`, or `stress`. `budget_bytes` is the
`spillBudgetBytes` installed for that row. `merge_passes` stays 0 for grouped sum, because that
path has no pass counter. sbt may still prefix lines. The objects themselves have no label.

### Method

Both cases use `columnarBatchSize` 8192, two source partitions, and two shuffle partitions.
Sort-merge keeps `spillFanIn` at its default, 8. The spill directory is a fresh directory under
`java.io.tmpdir`, named for this bench. `spillMaxFiles` and `spillMaxBytes` are pinned to the
`ScarletConf` defaults.

There are two budgets. The default is 64 MiB, which is what `spillBudgetBytes` is when nobody
sets it. Spill is enabled on that row. These inputs fit in the 64 MiB budget, so no files are
written. Off runs first in the same JVM, so that row does not measure the overhead of enabling
spill. The stress budget is 64 KiB. It is small enough that every exchanged grouped-sum bucket,
and both sides of every sort-merge partition, spill. The stress row is a worst case. The
default-budget row runs at width 1. Off and stress run at widths 1 and 4. Off installs the
default budget and does not consult it, so the table leaves that cell blank.

Grouped sum calls `DistCollection.ofIntPairs(...).groupedSum` on 1e6 rows. Keys are
`row % 16384`. The partial sum folds each source partition before the spill check, so the
exchanged bucket is what has to exceed the budget. At 16384 groups the stress budget is exceeded.
The same bucket fits in 64 MiB. A key set of 1024 still fits in 64 KiB and stays in memory.

Sort-merge calls `DistCollection.sortMergeJoin` on 147456 rows per side. Keys run from 0 and split
evenly across the two shuffle partitions, so each side of a partition is nine morsels of 8192,
one past the fan-in. The two sides together exceed 64 KiB and fit in 64 MiB. The merge depth is
two: nine runs, a fan-in of 8, then one more pass to finish the side.

Two warmup rounds are discarded. Five rounds are measured. Percentiles are nearest-rank: the
1-based rank is `ceil(p / 100 * n)`, with no interpolation. For 5 samples, p50 is rank 3, p95 is
rank 5, and max is rank 5. p95 is the largest sample.

The counters have to show the path before a time is kept. A `CountingSink` is installed outside
the timer. Every columnar run must publish at least one column batch, and the row oracle must
publish none, so a silent fall back to the row path fails the run. `SumSpill.resetForTest` runs
before each grouped attempt. `SortMergeRuns.resetForTest` runs before each sort-merge attempt.
Stress must publish the file count the harness derives from that row sizing: one morsel file per
piece of each spilled bucket, and, for sort-merge, one file per initial run plus one file per
merged group, on each side of each partition. Off and the default budget must publish zero files.
A sort-merge stress run must also complete the derived pass count. A miss fails the run, and no
table is printed.

`files_written` is a publish count, not a live-file count. `SumSpill` deletes each file before
the next morsel, so the store holds one. The sort-merge `merge_passes` figure is the summed
counter over partitions and sides. Two partitions, two sides, and two passes are 8 completions.
The merge depth is two.

Before warmup, the stress result is compared with the in-memory columnar result and with the row
path (`columnarExecution` false). Grouped sum compares the folded total. Sort-merge checks every
joined row against the generators (`left = key + 1`, `right = key * 3 + 7`, one row per key) and
compares an order-independent mix of `(key, left, right)`. Each measured result is compared
again. The comparison sits outside the timer. A mismatch fails the run.

`runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
JIT noise. Comparisons within one run are the signal. Widths 1 and 4 share one JVM. Off runs
before the default budget, then stress.

### Baseline

Recorded on an Apple M1 Pro (8 cores), OpenJDK 64-Bit Server VM 17.0.16, through `sbt` with
`Test / fork` left false, on 2026-10-28. Absolute numbers carry sbt and JIT noise. The comparison
that matters is within this run.

| case | spill | budget | threads | rows | p50 ms | p95 ms | max ms | files_written | merge_passes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| grouped-sum | off | — | 1 | 1000000 | 16.521 | 18.530 | 18.530 | 0 | 0 |
| grouped-sum | off | — | 4 | 1000000 | 8.530 | 19.557 | 19.557 | 0 | 0 |
| grouped-sum | on | 64 MiB | 1 | 1000000 | 16.072 | 18.081 | 18.081 | 0 | 0 |
| grouped-sum | on | 64 KiB | 1 | 1000000 | 234.846 | 251.688 | 251.688 | 22 | 0 |
| grouped-sum | on | 64 KiB | 4 | 1000000 | 235.964 | 271.095 | 271.095 | 22 | 0 |
| sort-merge | off | — | 1 | 147456 | 25.660 | 54.609 | 54.609 | 0 | 0 |
| sort-merge | off | — | 4 | 147456 | 11.363 | 19.016 | 19.016 | 0 | 0 |
| sort-merge | on | 64 MiB | 1 | 147456 | 11.563 | 12.339 | 12.339 | 0 | 0 |
| sort-merge | on | 64 KiB | 1 | 147456 | 492.002 | 536.562 | 536.562 | 44 | 8 |
| sort-merge | on | 64 KiB | 4 | 147456 | 513.245 | 542.938 | 542.938 | 44 | 8 |

Grouped sum with spill off is 16.521 ms at one thread and 8.530 ms at four. Spill is enabled at
the default 64 MiB budget and these inputs fit, so that row writes no files and measures 16.072 ms
at one thread. The 64 KiB stress row is 234.846 ms at one thread and 235.964 ms at four.
Sort-merge with spill off is 25.660 ms and 11.363 ms. Spill is enabled at the default budget and
these inputs fit, so that row writes no files and measures 11.563 ms. At 64 KiB it is 492.002 ms
and 513.245 ms. Off runs first in the same JVM, so the default-budget times are not the overhead
of enabling spill. The stress row is a worst case.

File and pass counts on the stress rows stay put when the width changes. Those rows show no width
scaling here, and the bench does not attribute the cost. Grouped sum publishes 22 files.
Sort-merge publishes 44 files and 8 merge passes. The grouped checksum is `499999500000`. The
sort-merge checksum is `7877949609597362176`, over 147456 pairs. Each matches the generators, the
in-memory columnar result, and the row path.

The table does not measure whole-operator memory. A spilled sort-merge merges at most
`spillFanIn` runs at a time, and the inputs and the joined result are still materialised.
Shuffle-hash joins are absent from the table. They do not spill. The inner broadcast build does
spill, and this bench does not run it. p95 is the largest of the five samples, so it is not a
stable tail.
