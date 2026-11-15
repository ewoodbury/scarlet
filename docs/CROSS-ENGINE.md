# Cross-engine checks: DuckDB and Spark

Not a CI gate. The CI gate is `PerfCheck`, which compares the columnar path with the row path in
the same run. The decision table is the 2026-11-15 steady-window run at `2b7b01e`. The warmup-5
full-window run at `8958ac9` is the previous scoreboard. The 2026-11-14 second run is the one
before that, at `ffca249`. The 2026-11-13 devbox section is the earlier warmup-3, rounds-10
protocol. The Apple M1 Pro table is history from 2026-11-07. It is not comparable with the
container or the devbox.

## 2026-11-15 — devbox, steady window

Decision table. Commit `2b7b01e`. Same machine as the warmup-5 section below.

- Machine: `sjc-gpu-01` (`nixos`), x86_64, AMD Ryzen 9 5900XT, 32 threads, 62 GB, OpenJDK
  21.0.13, sbt 1.10.11. The CPU governor is `powersave`. `sudo -n true` asks for a password, so
  the governor was left.
- Scarlet: 1e6 rows, 4 threads, 2 filters, no nulls, spill on. Each log booted sbt (`welcome to
  sbt`) and the first query started at about 3.5–4.0 s of JVM uptime. Warmup 80, rounds 30.
  `steady_ms` is the upper median of the last 10 rounds. `compilation` is kept and published as
  `steady_ms`. `machine` (the two halves of those last 10 differ by more than 10%) is dropped.
  The row is the upper median of the kept values, index `n/2` after sorting. Drop the five when
  fewer than three JVMs remain, or when `(max - min) / median` of the kept values is above 0.10.
- DuckDB 2.0.0, in memory, same rows, threads, and filters. Warmup 5, rounds 30. The engine is
  native, so the timer has no JIT compile-in: one extra process, not in the five below, had
  first-10 / last-10 median ratios of 1.07, 1.11, 1.04, 1.01, 0.89, and 1.01 across the six
  queries, and the join's tail was the slower half. A process before that five printed count at
  1.220 ms, so the column is the upper median of five processes, not one process. Sum's five
  p50s span 10.1% (0.979 to 1.084 ms). The other five queries span 0.6% to 9.6%. The published
  DuckDB number is still that median. The 10% drop rule is the Scarlet rule.

| query | DuckDB | Scarlet | ratio | DuckDB checksum | Scarlet checksum | kept |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + count | 1.020 ms | 0.732 ms | 1.39x ahead | 333036 | 333036 | 5 of 5 |
| filter + sum | 1.037 ms | — | — | 166553391274 | 166553391274 | none |
| filter + countByKey | 1.514 ms | 1.542 ms | 1.02x behind | 11147169 | 11147169 | 5 of 5 |
| filter + reduceByKey | 2.045 ms | 1.707 ms | 1.20x ahead | 166564205407 | 166564205407 | 3 of 5 |
| filter + join | 4.141 ms | 5.360 ms | 1.29x behind | 3330360 | 3330360 | 4 of 5 |
| filter + sort | 70.245 ms | 35.568 ms | 1.97x ahead | 5343278082631 | 5343278082631 | 5 of 5 |

Checksums matched on every Scarlet JVM and every DuckDB process. The harness still runs
`filter-groupbykey` in the same JVM, between sum and reduce. That query is not a row here. The
DuckDB script's `filter-groupbykey` is `count(*)` grouped by key, which is Scarlet's
`filter-countbykey`.

The first five is the published row when it passes. A later five is used only after an earlier
five was dropped. Count and join passed on the first five. CountByKey passed on the second.
`reduceByKey` and sort passed on the third. Sum did not pass.

### What was dropped

- filter + count, first five, kept. All five were `compilation`. Steady values 0.706, 0.713,
  0.732, 0.743, 0.779 ms. Median 0.732 ms. Span 9.97%.
- filter + sum, three fives, all dropped on span. Every JVM was `ok` or `compilation`. Medians
  0.666 ms (span 26.5%), 0.714 ms (span 12.9%), and 0.696 ms (span 17.3%). The fifteen steady
  readings run from 0.627 ms to 0.804 ms. All are ahead of DuckDB 1.037 ms, from 1.29x to 1.65x.
  There is no kept row.
- filter + countByKey, first five dropped. All five were `ok`, at 2.589, 2.709, 1.656, 1.622,
  and 1.557 ms. Span 69.5%. The second five is the row: 1.448, 1.508, 1.542, 1.587, 1.594 ms,
  one of them `compilation`, median 1.542 ms, span 9.4%, 5 of 5. A third five was 1.456–1.670 ms,
  span 13.6%, and was dropped.
- filter + reduceByKey, first two fives dropped. Each kept only two JVMs; the other three were
  `machine`. The third five dropped two `machine` JVMs (1.619 ms and 1.673 ms) and kept 1.694
  (`compilation`), 1.707, and 1.753 ms. Median 1.707 ms. Span 3.5%. 3 of 5.
- filter + join, first five, kept. Dropped one `machine` JVM at 5.389 ms. Kept 5.196, 5.241,
  5.360, and 5.388 ms. Median 5.360 ms. Span 3.6%. 4 of 5. Two later fives also passed, medians
  5.150 ms and 5.097 ms, and are not the row.
- filter + sort, first two fives dropped. All JVMs were `ok`, and one or two tails sat near 56 ms
  after a head near 35 ms (56.197 ms; then 55.728 ms and 57.088 ms). Spans 62% and 60%. The third
  five had no slow tail: 35.196, 35.508, 35.568, 35.960, 36.302 ms. Median 35.568 ms. Span 3.1%.
  5 of 5.

### Against the warmup-5 table

Protocols differ, and the tree moved from `8958ac9` to `2b7b01e`. Count, countByKey, and
`reduceByKey` changed in #225, #229, #231, and #232. Join and sort have no operator commit in
that range. #227 added a sum test and did not change a sum kernel.

- filter + count: 1.028 ms, 1.02x ahead, to 0.732 ms, 1.39x ahead.
- filter + sum: 0.888 ms, 1.23x ahead. No kept steady five. The readings sit below that p50 and
  stay ahead of DuckDB.
- filter + countByKey: 3.112 ms, 1.90x behind, to 1.542 ms, 1.02x behind.
- filter + reduceByKey: 4.082 ms, 2.00x behind, to 1.707 ms, 1.20x ahead.
- filter + join: 6.120 ms, 1.47x behind, to 5.360 ms, 1.29x behind.
- filter + sort: 36.184 ms, 1.92x ahead, to 35.568 ms, 1.97x ahead.

```shell
ssh nixos 'LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  ~/ce/bin/python ~/cross_engine_duckdb.py --rows 1000000 --threads 4 --warmup 5 --rounds 30'

ssh nixos 'cd ~/scarlet && CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false \
  CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 \
  sbt -batch -J-Xmx8g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"'
```

The DuckDB command ran five times. The Scarlet command ran five times per five, three fives in
all, from a detached checkout of `2b7b01e`. `Test / fork` is false, so each boot is the bench JVM.

## 2026-11-15 — devbox, warmup 5

Earlier decision table. Commit `8958ac9`. Same machine and the same paired protocol as the
2026-11-14 second run. Not the decision table.

- Machine: `nixos`, x86_64, 32 cores, 62 GB, OpenJDK 21.0.13, sbt 2. The CPU governor is
  `powersave`.
- Protocol: 1e6 rows, 4 threads, 2 filters, no nulls, warmup 5, rounds 30. DuckDB 2.0.0, one
  in-memory run. Scarlet is the median of five fresh JVMs. Spill is on in the Scarlet harness.
- Since `ffca249`: #221 folds a pair-filter count or sum over the source arrays and does not
  encode a batch. #224 counts an int shuffle-hash inner join by summing chain lengths and does
  not write the match columns. #223 replaced the previous copy of this scoreboard and did not
  change the engine.

| query | DuckDB | Scarlet | ratio | DuckDB checksum | Scarlet checksum |
| --- | ---: | ---: | ---: | ---: | ---: |
| filter + count | 1.049 ms | 1.028 ms | 1.02x ahead | 333036 | 333036 |
| filter + sum | 1.094 ms | 0.888 ms | 1.23x ahead | 166553391274 | 166553391274 |
| filter + countByKey | 1.634 ms | 3.112 ms | 1.90x behind | 11147169 | 11147169 |
| filter + reduceByKey | 2.046 ms | 4.082 ms | 2.00x behind | 166564205407 | 166564205407 |
| filter + join | 4.175 ms | 6.120 ms | 1.47x behind | 3330360 | 3330360 |
| filter + sort | 69.336 ms | 36.184 ms | 1.92x ahead | 5343278082631 | 5343278082631 |

### Against the `ffca249` table

- filter + count: 2.095 ms to 1.028 ms, 1.98x behind to 1.02x ahead. #221 folds the count on the
  source arrays. Inside the 1.5x target.
- filter + sum: 2.090 ms to 0.888 ms, 1.90x behind to 1.23x ahead. That fold sums the value
  column too. Inside the 1.5x target.
- filter + countByKey: 3.017 ms to 3.112 ms, 1.83x to 1.90x behind. Inside the previous five-JVM
  span (2.834–3.228 ms) and inside the 2.0x target.
- filter + reduceByKey: 4.111 ms to 4.082 ms, 1.86x to 2.00x behind. Scarlet stayed inside the
  previous span (3.891–4.266 ms). DuckDB's single run moved from 2.210 ms to 2.046 ms, which
  puts the ratio on the 2.0x target.
- filter + join: 14.649 ms to 6.120 ms, 3.49x to 1.47x behind. #224 counts the matches and does
  not write them. DuckDB's in-memory run moved from 4.193 ms to 4.175 ms. Inside the 2.0x target
  on this median.
- filter + sort: 35.794 ms to 36.184 ms, 1.94x to 1.92x ahead. Inside the previous span
  (35.120–37.202 ms).

Protocol: 1e6 rows, 4 threads, two filters, no nulls, warmup 5, 30 rounds. Scarlet is the median
of five fresh JVMs. DuckDB is one in-memory run. The join median is 6.120 ms and sits inside
5.9–7.1 ms. The five p50s are 5.736, 5.979, 6.120, 7.045, and 10.715 ms. The 10.715 ms sample is
the JVM that compiled three columnar sources before the rounds. Two later fives of the same
command, after this harness had already run, median'd at 10.359 ms (span 6.337–10.884) and
10.997 ms (span 8.112–11.223).

```shell
ssh nixos 'LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  ~/ce/bin/python ~/cross_engine_duckdb.py --rows 1000000 --threads 4 --warmup 5 --rounds 30'

ssh nixos 'cd ~/scarlet && CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false \
  CROSS_WARMUP=5 CROSS_ROUNDS=30 \
  sbt -batch -J-Xmx8g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"'
```

The Scarlet command ran five times. Each log booted sbt from `welcome to sbt`, and the table is
the median of those five p50s.

### Stability

The join publishes above are 6.120 ms, then 10.359 ms, then 10.997 ms for one command. 10.997 /
6.120 is 1.80×. That is larger than the kernel gaps this scoreboard is trying to see. The
devbox governor was `powersave`. This container is aarch64, 4 cores, Vendor ID Apple, OpenJDK
21.0.13. `/sys/devices/system/cpu/cpu0/cpufreq` is absent and `sudo` is absent, so the governor
was left unset.

A cold `filter-join` (warmup 0, 40 rounds, `java -Xmx4g -XX:+UseG1GC`) opens at 289.5 ms.
Sample indexes 16–19 are 6.8, 6.1, 6.2, and 7.1 ms. `PrintCompilation` on that shape shows
`HashExchange` from uptime 1146 ms, inside the 289 ms first round (`QUERY_BEGIN` uptime 889
ms), and `JoinSlots` at 1330 ms while later rounds are still falling. Eight
full-suite JVMs at warmup 5 and rounds 30 all kept falling after round 5: the median of the
first 10 rounds was 1.29× to 2.06× the median of the last 10. The full-window p50 sits on that
slope, which is how one JVM lands near 6 ms and another near 10 ms. A cold `filter-count`
compiles in round 0 only: 120.35 ms, then 2.60 ms. A later jump, such as a join whose last 10
rounds split 10.3 ms / 7.2 ms, is the machine.

Warmup 80 and rounds 30 on the full suite, five fresh JVMs, same heap and GC. Join JVM 1 was
`machine` (last-10 halves 10.292 ms and 7.221 ms) and is dropped. The other four `steady_ms`
values are 4.682, 4.758, 4.796, and 4.870 ms. Their median is 4.796 ms and the span is 3.9%.
Count drops one JVM whose last-10 halves are 2.010 ms and 1.389 ms. The kept `steady_ms`
values are 1.099, 1.113, 1.129, and 1.165 ms. Their median is 1.129 ms and the span is 5.8%.
Checksums on every JVM were count `333036` and join `3330360`, the same formulas as the table.
These milliseconds are this container. They are not the devbox steady-window table.

The harness prints `STEADY` and writes `steady_ms` plus `steady`. The rule:

- Warmup 80, rounds 30, five fresh JVMs. `CROSS_WARMUP` and `CROSS_ROUNDS` now default to
  those. `steady_ms` is the upper median of the last 10 rounds.
- `compilation`: the first 10 are more than 1.10× the last 10. Publish `steady_ms`. The
  full-window `p50_ms` still contains the slope.
- `machine`: the two halves of the last 10 differ by more than 10%. Drop that JVM.
- `short`: fewer than 20 rounds, so the line has no separate tail. Do not publish.
- Publish the median of the kept `steady_ms` values. Drop the set when `(max - min) / median`
  is above 0.10, or when fewer than three JVMs remain.
- If `scaling_governor` reads `powersave` and `sudo -n true` succeeds, set `performance` for
  the run and record it. Otherwise record the governor and leave it.

The Scarlet milliseconds in this section are warmup-5 full-window p50s. They are not the
decision table. The steady-window section is the one that applies this rule. The checksums
stay on the formulas in that table.

## 2026-11-14 — devbox, second run

Earlier decision table. Commit `ffca249`. Same machine as the 2026-11-13 section, paired protocol.
Not the decision table.

- Machine: `nixos`, x86_64, 32 cores, 62 GB, OpenJDK 21.0.13, sbt 2.
- Protocol: 1e6 rows, 4 threads, 2 filters, no nulls, warmup 5, rounds 30. DuckDB 2.0.0, one
  in-memory run. Scarlet is the median of five fresh JVMs. Spill is on in the Scarlet harness.
- Since `cededb0`: join output rewrite (#215), reduceByKey fold (#216), reduce phases (#214),
  reduceByKey partial-table sizing (#220), and the countByKey key copy (#222). Long and string
  inner sort-merge (#211, #217) and map above `groupByKey` (#213) do not run on this int suite.
  #219 added a phase bench and did not change these actions.

| query | DuckDB | Scarlet | ratio |
| --- | ---: | ---: | ---: |
| filter + count | 1.060 ms | 2.095 ms | 1.98x behind |
| filter + sum | 1.102 ms | 2.090 ms | 1.90x behind |
| filter + countByKey | 1.653 ms | 3.017 ms | 1.83x behind |
| filter + reduceByKey | 2.210 ms | 4.111 ms | 1.86x behind |
| filter + join | 4.193 ms | 14.649 ms | 3.49x behind |
| filter + sort | 69.435 ms | 35.794 ms | 1.94x ahead |

Checksums matched on all six queries: count `333036`, sum `166553391274`, countByKey `11147169`,
reduceByKey `166564205407`, join `3330360`, sort `5343278082631`.

The 2026-11-13 table is warmup 3 and rounds 10. These medians are warmup 5 and rounds 30, the
same protocol as the `cededb0` scoreboard (count 2.14x, sum 2.06x, countByKey 2.71x, reduceByKey
3.51x, join 3.50x, sort 1.91x ahead).

### Read

Count stayed inside this run's five-JVM span (1.891–2.253 ms; `cededb0` was 2.148 ms). The ratio
moved from 2.14x to 1.98x and is outside the 1.5x target. Sum got slower, 2.016 ms to 2.090 ms,
and every sample this run was at least 2.061 ms. The ratio moved from 2.06x to 1.90x because
DuckDB moved from 0.981 ms to 1.102 ms, and 1.90x is outside the 1.5x target. `countByKey` moved
outside noise, 4.085 ms to 3.017 ms, 2.71x to 1.83x, inside the 2.0x target. `reduceByKey` moved
outside noise, 7.071 ms to 4.111 ms, 3.51x to 1.86x, inside the 2.0x target. Join did not move
outside noise: the median is 14.649 ms against 14.231 ms, and the five JVMs span 12.449–20.645 ms,
so the ratio stays 3.49x and outside the 2.0x target. Sort stayed inside noise (35.794 ms against
35.988 ms) and stayed ahead, 1.94x.

```shell
ssh nixos 'LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  ~/ce/bin/python ~/cross_engine_duckdb.py --rows 1000000 --threads 4 --warmup 5 --rounds 30'

ssh nixos 'cd ~/scarlet && CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false \
  CROSS_WARMUP=5 CROSS_ROUNDS=30 \
  sbt -batch -J-Xmx8g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"'
```

The Scarlet command ran five times. Each log booted sbt from `welcome to sbt`, and the table is
the median of those five p50s.

## 2026-11-13 — devbox, both engines, one machine

Earlier protocol (warmup 3, rounds 10). Both engines on one machine. Not the decision table.

- Machine: `nixos`, x86_64, 32 cores, 62 GB, OpenJDK 21.0.13, sbt 2.
- Protocol: 1e6 rows, 4 threads, warmup 3, rounds 10. DuckDB 2.0.0 in memory. Scarlet with spill
  enabled by its harness.
- All seven checksums matched on both engines.

| query | DuckDB | Scarlet | ratio |
| --- | ---: | ---: | ---: |
| filter + count | 1.021 ms | 2.456 ms | 2.4x behind |
| filter + sum | 0.966 ms | 2.529 ms | 2.6x behind |
| filter + countByKey | 1.574 ms | 4.790 ms | 3.0x behind |
| filter + reduceByKey | 2.070 ms | 9.888 ms | 4.8x behind |
| filter + join | 4.640 ms | 27.039 ms | 5.8x behind |
| filter + sort | 69.832 ms | 61.706 ms | 1.13x ahead |

Scarlet's harness enables spill while DuckDB is in memory. Neither engine is tuned to its best
configuration. The ratio is like-for-like on the query, not on the deployment.

```shell
# DuckDB side (nix python needs libstdc++ from the nix store)
ssh nixos 'LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  ~/ce/bin/python ~/cross_engine_duckdb.py --rows 1000000 --threads 4 --rounds 10'

# Scarlet side
ssh nixos 'cd ~/scarlet && CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false \
  CROSS_WARMUP=3 CROSS_ROUNDS=10 \
  sbt -batch -J-Xmx8g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"'
```

## Setup

Historical Apple M1 Pro setup for the 2026-11-07 table below. That table is not comparable with
the container or the devbox. Its ratios are history. Current ratios are in
"2026-11-15 — devbox, steady window".

One-time comparison, 2026-11-07.

- Machine: Apple M1 Pro, 8 cores, 16 GB.
- DuckDB 2.0.0, in-memory, `PRAGMA threads=4`.
- Spark 4.3.0, `local[4]`, `spark.sql.shuffle.partitions=4`, both tables cached.
- Scarlet: 4 threads, 2 source partitions, batch 8192, columnar execution on.
- 1,000,000 rows. `k = row % 1024`, `v` from the `QueryBench` LCG, `sk` a deterministic
  Fisher-Yates permutation, and a 10,240-row join side with 10 rows per key.
- Warmup 3, measured 10, p50. The timed region is the action; data generation and table creation
  are outside it.

## Queries at 1M rows

Historical Apple M1 Pro table, 2026-11-07. Not comparable with the container or the devbox.
Current ratios are in "2026-11-15 — devbox, steady window".

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

All seven checksums match on all three engines. These ratios are the 2026-11-07 M1 Pro history,
not the decision table: Scarlet was 1.9x slower on the sum, 2.6x on the count, 4.8x on the count
per key, 3.3-4.8x on the reduce, 4.6x on the join, and 0.78x on the sort. The 2026-11-13
shorter-protocol ratios were count 2.4x behind, sum 2.6x behind, countByKey 3.0x behind,
reduceByKey 4.8x behind, join 5.8x behind, sort 1.13x ahead. Current ratios are in
"2026-11-15 — devbox, steady window".

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
finish. Its numbers are in the next section. They do not replace the historical M1 1M table above.

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

The 2-5x gap here is the 2026-11-07 M1 Pro history (count 2.6x, sum 1.9x, countByKey 4.8x, reduce
3.3-4.8x, join 4.6x, sort 0.78x), not the decision table. Current ratios are in
"2026-11-15 — devbox, steady window". On that M1 run the gap to DuckDB on scans,
aggregates and joins was the scalar JVM kernel plus the source encode, not representation. Two
measured spikes killed the earlier explanations: a 10-bit packed column scans slower than a raw
int array at this size (1.91 vs 0.84 ns/row), and the Panama
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
`CROSS_NULLS` to false. The command above pins warmup 3 and rounds 10. The harness defaults
are warmup 80 and rounds 30, so a steady tail is visible. `steady_ms` is the median of the
last 10 rounds; publish that when `steady` is `ok` or `compilation`. Set `CROSS_QUERY`
to one of `filter-count`, `filter-sum`, `filter-groupbykey`, `filter-reducebykey`, `filter-join`,
`filter-sort`, or `filter-countbykey`. Omit it to run all seven in that process, dropping each
query's input before the next. `encode_ms` is reported separately and is not subtracted from the
action. The action timer still includes the scan encode. Spill is on for this harness only.

The Python harnesses live outside the repo; the Scarlet harness is
`modules/scarlet-tests/src/test/scala/com/ewoodbury/scarlet/execution/CrossEngineBench.scala`.

### Scarlet-only `filter-countbykey`

Recorded on 2026-11-10 on Linux 6.8.0-117-generic, aarch64, 4 cores (Vendor ID Apple), OpenJDK
64-Bit Server VM 21.0.13, through `sbt -J-Xmx2g` with `Test / fork` left false. Protocol:
`CROSS_ROWS=1000000`, `CROSS_FILTERS=2`, `CROSS_NULLS=false`, `CROSS_WARMUP=3`,
`CROSS_ROUNDS=10`, 4 threads, batch 8192, spill on. `encode_ms` is the one-shot encode before
warmup. It is not part of the p50, and the p50 still includes the scan encode. The harness p50
is `samples(rounds / 2)` after sorting, the same selector as the spilled-harness tables above.
`encode_ms` here is one cold encode, so it overstates the encode share: the same encoder over the
same survivor rows times 1.471 ms warm in `PartialExchangeBench` (see `BENCHMARKS.md`). The 12.65 ms
action is therefore not mostly encode, and neither figure should be subtracted from the other. Do not subtract this p50 from
the 7.05 ms `QueryBench` figure or from the 1.46 ms DuckDB figure. `filter-join` was not
re-run here. Its encode split remains the 2026-11-08 spilled-harness row.

| Query | p50 | encode |
| --- | ---: | ---: |
| filter + countByKey | 12.65 ms | 9.65 ms |

The printed values were 12.649646 ms and 9.649068 ms. The run published 64 column batches.
Checksum `11147169` is the sum of `key * 31 + count` over each `countByKey` pair. That is the
cross-engine harness formula, not the `QueryBench` per-group xor.
