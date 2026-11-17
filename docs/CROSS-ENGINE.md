# Cross-engine checks: DuckDB and Spark

Not a CI gate. The CI gate is `PerfCheck`, which compares the columnar path with the row path in
the same run. The decision table for the original six is the 2026-11-17 pinned run at
`50591f6`. The materialising join is the section at `7955dca`. The four shapes added in #242
are the section at `8bfc7ca`. The 2026-11-16 pinned
run at `b704c3c` (#234) is the previous scoreboard. The 2026-11-15 steady-window table (#233)
stops at `2b7b01e`, before #234. The warmup-5 full-window run at `8958ac9` is the one before that. The
2026-11-14 second run is at `ffca249`. The 2026-11-13 devbox section is the earlier warmup-3,
rounds-10 protocol. The Apple M1 Pro table is history from 2026-11-07. It is not comparable with
the container or the devbox.

## 2026-11-17 — materialising join, pinned

`filter-joinsum` on `sjc-gpu-01`, commit `7955dca`. This is not the count row in the decision
table. That row is `MatchCount`: it sums chain lengths and writes no columns, on an earlier
binary (`50591f6`). This action is the same int inner join, then `sum(left * 31 + right)` over
the value columns the join writes. The box clock at the start of the timed runs was 2026-11-17
08:38 PT. Governor `powersave`, left. Pin `taskset -c 0,2,4,6`. OpenJDK 21.0.13.0.2, classes
from sbt 1.10.11. The timed Scarlet process is `java -Xmx10g -cp` on that classpath, not an sbt
JVM. Four threads, two filters, no nulls, spill on for the process.

Scarlet is five fresh JVMs. Warmup 80, rounds 30. `steady_ms` is the upper median of the last
10 rounds, index `n/2` after sorting. `compilation` is kept. `machine` is dropped. The row is
the upper median of the kept values. Span is `(max - min) / median` of those values. DuckDB
2.0.0, in memory, warmup 5, rounds 30, five processes. `p50_ms` is the mean of the two central
round times, then the column is the upper median of those five. The steady tags are not applied
to DuckDB.

The plan that ran, on every JVM, is
`HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))`. `batches` was 32 and
`fused` was 0, so the count fold did not run and the row path did not run. DuckDB's statement
is `SELECT sum(t.v::BIGINT * 31 + u.w::BIGINT) FROM t JOIN u ON t.k = u.j WHERE t.k % 3 != 0 AND t.v >= 0`.
The build keys are `row % 1024`, the probe is ten copies of those keys, and the filters are
`k % 3 != 0` and `v >= 0`, the same inputs as `filter-join`.

Checksums matched on every JVM and every DuckDB process, including the first 1e7 set. That
first five kept 5 of 5 and spanned 14.2%: one JVM at 126.581 ms, the other four at
110.494–114.173 ms. A second five, started at load 0.48, spans 14.0% for the same reason, one
JVM at 125.253 ms, and the other four at 109.806–113.919 ms. Both sets stay over the 10% line,
so the row is published with that span. The value is the second five's upper median, 110.700 ms,
which sits in the cluster of four. 1e6 kept 5 of 5 and spanned 7.9%.

### 1e6 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + join sum | 5.979 ms | 0.4% | 8.897 ms | 7.9% | 1.49x behind | 5 of 5 | 51631715699610 | 60.191 MB |

### 1e7 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + join sum | 55.522 ms | 0.1% | 110.700 ms | 14.0% | 1.99x behind | 5 of 5 | 515818008297280 | 593.811 MB |

The count row on `50591f6` is 3.86x ahead at 1e6 and 3.91x ahead at 1e7. Those times are a
different action on an earlier binary. This row is the join that returns its values: 1.49x
behind DuckDB at 1e6, 1.99x behind at 1e7. The action writes the match columns and sums the
value pair. The count row does not write those columns.

```shell
# DuckDB. Five processes. --query filter-joinsum is the statement above.
# Repeat with --rows 10000000.
LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  taskset -c 0,2,4,6 ~/ce/bin/python ~/cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30 --query filter-joinsum

# Scarlet. One JVM. Repeat five times, and again at CROSS_ROWS=10000000.
# The 1e7 row is the second five.
CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-joinsum CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_TIME_ROW=true \
  taskset -c 0,2,4,6 java -Xmx10g -cp "$CP" \
  com.ewoodbury.scarlet.execution.CrossEngineBench
```

## 2026-11-17 — four shapes, pinned

`distinct`, `topn`, `twogroup`, and `keymod` on `sjc-gpu-01`, commit `8bfc7ca` (#264). The
six-query section below is the earlier binary, `50591f6`. The box clock at the start was 2026-11-17
01:18 PT. Governor `powersave`, left. Pin `taskset -c 0,2,4,6`. OpenJDK 21.0.13.0.2, sbt 1.10.11.
The timed Scarlet process is `java -Xmx10g -cp` on that classpath, not an sbt JVM. Four threads,
two filters, no nulls, spill on for the process.

Scarlet is five fresh JVMs per query, one query per JVM. Warmup 80, rounds 30. `steady_ms` is the
upper median of the last 10 rounds, index `n/2` after sorting. `compilation` is kept. `machine`
is dropped. The row is the upper median of the kept values. Span is `(max - min) / median` of
those values. A set with fewer than three kept JVMs, or a span above 10%, is dropped.

DuckDB 2.0.0, in memory, warmup 5, rounds 30, five processes at each size. Each process ran the
four statements in `cross_engine_duckdb.py`, in order: `filter-distinct`, `filter-topn`,
`filter-twogroup`, `filter-keymod`. `p50_ms` is the mean of the two central round times, then the
column is the upper median of those five p50s. The steady tags are not applied.

`filter-distinct` is `keys.distinct.count`. The arm is
`Dedupe(Exchange(Key(FilterValue(FilterKey(Scan)))))`. `filter-topn` times `take(100)` on
`sortByIntValueDesc`. The harness records the sort plan as the arm,
`SortByIntValueDesc(FilterValue(FilterKey(Scan)))`; the action is the bounded heap, not the full
sort. `filter-twogroup` is the count and the sum over `groupByKey`. The arm is
`GroupedDecode(GroupByKey(Exchange(FilterValue(FilterKey(Scan)))))`. `filter-keymod` maps the key
to `k % 100` and then `countByKey`. That map changes the key, so the planner declines and the arm
is `row`. `CROSS_TIME_ROW=true` times the decline. The other three lower, so their arm stays the
columnar shape.

Checksums matched on every JVM and every DuckDB process, including the JVMs the steady rule
dropped. The first `distinct` window at 1e6 kept 5 of 5 and spanned 116%: one JVM at 11.664 ms,
the other four at 5.340–5.492 ms. That set is dropped. The row below is a second five, same
warmup and rounds, span 5.5%. `machine` dropped two `twogroup` JVMs at 1e6, one `twogroup` JVM
at 1e7, and one `keymod` JVM at 1e7.

### 1e6 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + distinct | 1.300 ms | 3.5% | 5.218 ms | 5.5% | 4.01x behind | 5 of 5 | 682 | 31.524 MB |
| filter + topn | 1.599 ms | 0.5% | 4.557 ms | 4.2% | 2.85x behind | 5 of 5 | 1460799775 | 16.315 MB |
| filter + twogroup | 1.859 ms | 1.8% | 8.928 ms | 4.0% | 4.80x behind | 3 of 5 | 166898953513 | 73.129 MB |
| filter + keymod | 1.744 ms | 1.3% | 66.547 ms | 9.9% | 38.16x behind, row path | 5 of 5 | 486486 | 233.939 MB |

### 1e7 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + distinct | 9.681 ms | 0.9% | 62.095 ms | 5.4% | 6.41x behind | 5 of 5 | 682 | 309.608 MB |
| filter + topn | 9.622 ms | 2.8% | 47.137 ms | 8.4% | 4.90x behind | 5 of 5 | 15750242131 | 162.907 MB |
| filter + twogroup | 11.304 ms | 0.5% | 89.736 ms | 4.8% | 7.94x behind | 4 of 5 | 1664362168978 | 705.531 MB |
| filter + keymod | 12.387 ms | 0.8% | 1203.643 ms | 5.8% | 97.17x behind, row path | 4 of 5 | 3481589 | 2331.177 MB |

`keymod` is the row the columnar path refuses. At 1e6 the row path is 38x DuckDB. At 1e7 it is
97x, and it allocates 2331 MB where the DuckDB query is a grouped count.

```shell
# DuckDB. Five processes. --query four runs the four statements above, in that order.
# The stock script's --query takes one name; the run used a copy whose only change is that
# selection. The SQL and the checksum are the script's.
LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  taskset -c 0,2,4,6 ~/ce/bin/python ~/cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30 --query four

# Scarlet. One query per JVM. Repeat for each shape and for CROSS_ROWS=10000000.
# CROSS_TIME_ROW times a decline. A plan that lowers ignores it.
CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-distinct CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_TIME_ROW=true \
  taskset -c 0,2,4,6 java -Xmx10g -cp "$CP" \
  com.ewoodbury.scarlet.execution.CrossEngineBench
```

## 2026-11-17 — devbox, pinned

Decision table. Commit `50591f6` (#258). The box clock at the start was 2026-11-16 17:03 PT.
Same machine as the 2026-11-16 section. That section is not this binary: #239, #240, #243, #247,
and #236 landed after `b704c3c`.

- Machine: `sjc-gpu-01` (`nixos`), x86_64, AMD Ryzen 9 5900XT, 32 threads, 62 GB, OpenJDK
  21.0.13.0.2. The CPU governor was `powersave` and was left. Cores 0, 2, 4, and 6 are distinct
  physical cores.
- Pin: `taskset -c 0,2,4,6` on every Scarlet JVM, every DuckDB process, and every DataFusion
  process.
- Scarlet: 4 threads, 2 filters, no nulls, spill on. Classes compiled with sbt 1.10.11. The timed
  process is `java -Xmx10g -cp` on that classpath, not an sbt JVM. Warmup 80. `steady_ms` is the
  upper median of the last 10 rounds, index `n/2` after sorting. `compilation` is kept.
  `machine` (the two halves of those last 10 differ by more than 10%) is dropped. The row is the
  upper median of the kept values. Span is `(max - min) / median` of those values.
- Rows under 2 ms, and any 1e6 row whose rounds-30 window was not flat, were taken at rounds 100
  across nine JVMs. Sort at 1e6, and the 1e7 rows, stay at rounds 30. Count and sum at 1e7, and
  join at 1e7, are nine JVMs because the first five missed the 10% span or kept fewer than five.
  The other 1e7 rows are five JVMs.
- DuckDB 2.0.0, in memory, same rows, threads, filters, and pin. Warmup 5, rounds 30. Nine
  processes at 1e6, five at 1e7. Each process ran the six decision queries, in order: count, sum,
  countByKey, reduceByKey, join, sort. The steady tags are not applied. `p50_ms` is the mean of
  the two central round times, then the column is the upper median of those process p50s.
- DataFusion 55.0.0, in memory, same shape and the same process counts. The timer is `collect()`.
  DuckDB's timer is `fetchall()`. Sort is the row where that difference is the result size.

Checksums matched on every kept JVM and every DuckDB and DataFusion process.

### 1e6 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |
| filter + count | 0.941 ms | 3.9% | 0.731 ms | 9.8% | 1.29x ahead | 8 of 9 | 333036 | 0.052 MB |
| filter + sum | 0.995 ms | 1.8% | 0.732 ms | 12.7% | 1.36x ahead | 6 of 9 | 166553391274 | 0.052 MB |
| filter + countByKey | 1.524 ms | 2.8% | 2.099 ms | 9.1% | 1.38x behind | 7 of 9 | 11147169 | 0.680 MB |
| filter + reduceByKey | 2.023 ms | 3.1% | 1.757 ms | 42.4% | 1.15x ahead | 5 of 9 | 166564205407 | 0.600 MB |
| filter + join | 4.163 ms | 0.8% | 1.077 ms | 10.4% | 3.86x ahead | 5 of 9 | 3330360 | 0.449 MB |
| filter + sort | 70.735 ms | 3.4% | 13.898 ms | 5.5% | 5.09x ahead | 9 of 9 | 5343278082631 | 70.296 MB |

Count, sum, countByKey, reduceByKey, and join are rounds 100. Sort is rounds 30. Sum's kept
windows run 0.695–0.788 ms, so the span stays over 10% after nine JVMs at 100 rounds. Join's five
kept windows run 1.059–1.172 ms, just over the line. `reduceByKey`'s five kept windows are 1.024,
1.736, 1.757, 1.768, and 1.770 ms. A further nine JVMs at rounds 200 were not flat (eight
`machine`), and those tails sat near 1.2 ms, so 1.757 ms is the rounds-100 median and not a
settled floor.

### 1e7 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum | alloc |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: |
| filter + count | 7.018 ms | 0.8% | 6.188 ms | 12.9% | 1.13x ahead | 9 of 9 | 3328139 | 0.477 MB |
| filter + sum | 7.547 ms | 1.9% | 6.052 ms | 11.4% | 1.25x ahead | 9 of 9 | 1663923758546 | 0.477 MB |
| filter + countByKey | 10.242 ms | 0.8% | 9.905 ms | 3.9% | 1.03x ahead | 4 of 5 | 14142272 | 3.119 MB |
| filter + reduceByKey | 11.100 ms | 0.8% | 8.476 ms | 1.8% | 1.31x ahead | 5 of 5 | 1663934572679 | 3.038 MB |
| filter + join | 35.769 ms | 1.0% | 9.158 ms | 8.6% | 3.91x ahead | 7 of 9 | 33281390 | 0.652 MB |
| filter + sort | 687.482 ms | 0.8% | 139.173 ms | 1.3% | 4.94x ahead | 5 of 5 | 518322534674927 | 702.038 MB |

Count and sum are over the 10% line. Both medians are about 6 ms, so the under-2 ms rule does
not apply. Two count windows sit near 5.6 ms and the other seven sit at 6.10–6.35 ms. Sum is the
same shape: two near 5.7 ms, seven at 6.02–6.41 ms.

### DataFusion

| query | 1e6 | span | 1e7 | span | checksum 1e6 | checksum 1e7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| filter + count | 1.295 ms | 8.3% | 8.276 ms | 7.1% | 333036 | 3328139 |
| filter + sum | 1.328 ms | 6.5% | 9.141 ms | 5.3% | 166553391274 | 1663923758546 |
| filter + countByKey | 2.173 ms | 9.4% | 13.416 ms | 3.2% | 11147169 | 14142272 |
| filter + reduceByKey | 2.209 ms | 11.7% | 14.415 ms | 3.2% | 166564205407 | 1663934572679 |
| filter + join | 3.972 ms | 3.0% | 32.281 ms | 1.8% | 3330360 | 33281390 |
| filter + sort | 10.033 ms | 2.9% | 100.381 ms | 1.9% | 5343278082631 | 518322534674927 |

`reduceByKey` at 1e6 spans 11.7%. Sort is not a DuckDB ratio: DuckDB materialises the ordered
rows into Python on every round, and DataFusion does not.

### Against the `b704c3c` table

Count and sum stayed within a few tenths of a millisecond. No count or sum kernel landed after
that table.

- filter + countByKey, 1e6: 2.945 ms, 1.95x behind, to 2.099 ms, 1.38x behind. At 1e7: 40.288 ms,
  3.91x behind, to 9.905 ms, 1.03x ahead. Alloc on the 1e7 row is 3.119 MB. #239.
- filter + reduceByKey, 1e6: 2.694 ms, 1.33x behind, to 1.757 ms at the rounds-100 median, with
  the 42% span above. At 1e7: 13.109 ms, 1.18x behind, to 8.476 ms, 1.31x ahead. #243.
- filter + join, 1e6: 5.062 ms, 1.22x behind, to 1.077 ms, 3.86x ahead. At 1e7: 52.703 ms, 1.47x
  behind, to 9.158 ms, 3.91x ahead. #236 selects the build. #240 counts matches and does not copy
  the values.
- filter + sort, 1e6: 34.527 ms, 2.04x ahead, to 13.898 ms, 5.09x ahead. At 1e7: 476.492 ms,
  1.44x ahead, to 139.173 ms, 4.94x ahead. #247 radix-sorts present pairs. This query has no
  nulls, so it takes that path.

```shell
# One DuckDB process. DataFusion is the same flags on cross_engine_datafusion.py.
# This table's processes ran only the six names above, in that order.
LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  taskset -c 0,2,4,6 ~/ce/bin/python ~/cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30

# One Scarlet JVM. 1e6 count, sum, countByKey, reduceByKey, and join used rounds 100.
CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-count CROSS_WARMUP=80 CROSS_ROUNDS=100 \
  taskset -c 0,2,4,6 java -Xmx10g -cp "$CP" \
  com.ewoodbury.scarlet.execution.CrossEngineBench
```

## 2026-11-16 — devbox, pinned

Previous scoreboard. Commit `b704c3c` (#234). Same machine as the steady-window section below. The
protocol difference from that section is the pinning. `b704c3c` is the straight-line dense scan,
which lands after `2b7b01e`. The JVM was `java -cp` on those classes. #233 booted sbt in the bench JVM.
#236 is not in these numbers.

- Machine: `sjc-gpu-01` (`nixos`), x86_64, AMD Ryzen 9 5900XT, 32 threads, 62 GB, OpenJDK
  21.0.13. The CPU governor is `powersave`. `sudo -n true` asks for a password, so the governor
  was left. A pinned core can still sit at the 1740 MHz base. Cores 0, 2, 4, and 6 are distinct
  physical cores; their SMT siblings are 16, 18, 20, and 22.
- Pin: `taskset -c 0,2,4,6` on every Scarlet JVM and every DuckDB process.
- Scarlet: 4 threads, 2 filters, no nulls, spill on. Warmup 80, rounds 30, five fresh JVMs,
  except filter + count at 1e6. `steady_ms` is the upper median of the last 10 rounds.
  `compilation` is kept. `machine` (the two halves of those last 10 differ by more than 10%) is
  dropped. The row is the upper median of the kept values, index `n/2` after sorting. Drop the
  set when fewer than three JVMs remain, or when `(max - min) / median` of the kept values is
  above 0.10. Span in the table is that ratio. An action whose steady median is under 2 ms and
  whose five-JVM span is over 10% is retaken with more rounds until the kept span is inside
  10%. Count and `reduceByKey` are not dropped when the rule fires on a one-millisecond action.
- DuckDB 2.0.0, in memory, same rows, threads, filters, and pin. Warmup 5, rounds 30, five
  processes. The engine is native and has no JIT compile-in, so the steady tags are not applied.
  The column is the upper median of the five process p50s. An earlier pass in this sitting was
  unpinned and one process (count 1.002 ms at 1e6). The column above replaces that pass.

### 1e6 rows

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: |
| filter + count | 0.929 ms | 1.9% | 0.691 ms | 8.9% | 1.34x ahead | 9 of 9 | 333036 |
| filter + sum | 0.991 ms | 1.8% | 0.718 ms | 8.5% | 1.38x ahead | 5 of 5 | 166553391274 |
| filter + countByKey | 1.513 ms | 1.0% | 2.945 ms | 9.6% | 1.95x behind | 5 of 5 | 11147169 |
| filter + reduceByKey | 2.023 ms | 1.8% | 2.694 ms | 27.2% | 1.33x behind | 5 shown | 166564205407 |
| filter + join | 4.166 ms | 1.0% | 5.062 ms | 3.1% | 1.22x behind | 5 of 5 | 3330360 |
| filter + sort | 70.437 ms | 1.3% | 34.527 ms | 6.1% | 2.04x ahead | 5 of 5 | 5343278082631 |

Checksums matched on every kept JVM and every DuckDB process. Sum is under 2 ms and the five
already span 8.5%, so it was not retaken. `countByKey` spans 9.6% of its median (10.2% of its
minimum) and stays.

Filter + count, rounds 30, five JVMs, all `compilation`: 0.727, 0.727, 0.742, 0.744, 0.928 ms.
Median 0.742 ms. Span 27.2%. Dropped. Nine JVMs at rounds 100 left eight `machine` and one
`compilation` (0.773 ms); the set was dropped. Nine JVMs at rounds 200, warmup 80, all
`compilation`: 0.676, 0.678, 0.681, 0.689, 0.691, 0.692, 0.694, 0.703, 0.737 ms. Median
0.691 ms. Span 8.9%. 9 of 9. Checksum `333036` on every JVM. That nine is the row.

Filter + `reduceByKey`, five steady values: 2.562 ms `machine`, 2.583 ms `compilation`,
2.694 ms `machine`, 3.048 ms `ok`, 3.296 ms `machine`. The machine rule leaves two JVMs, so the
set would drop. The row is kept. The median of the five is 2.694 ms, span 27.2%, 1.33x behind
DuckDB 2.023 ms. The action is 2.7 ms. A stall in one half of the last 10 moves that median by
more than 10%. That is the drop rule on an action this short.

### 1e7 rows

Same pin and the same five JVMs at rounds 30. No retake. Every DuckDB span is under 1.2%.

| query | DuckDB | span | Scarlet | span | ratio | kept | checksum |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: |
| filter + count | 7.009 ms | 0.7% | 5.784 ms | 11.7% | 1.21x ahead | 5 of 5 | 3328139 |
| filter + sum | 7.495 ms | 1.1% | 6.109 ms | 9.5% | 1.23x ahead | 5 of 5 | 1663923758546 |
| filter + countByKey | 10.302 ms | 1.2% | 40.288 ms | 6.1% | 3.91x behind | 5 of 5 | 14142272 |
| filter + reduceByKey | 11.128 ms | 1.0% | 13.109 ms | 6.8% | 1.18x behind | 5 of 5 | 1663934572679 |
| filter + join | 35.919 ms | 0.7% | 52.703 ms | 4.0% | 1.47x behind | 5 of 5 | 33281390 |
| filter + sort | 686.141 ms | 0.5% | 476.492 ms | 2.7% | 1.44x ahead | 5 of 5 | 518322534674927 |

Count at 1e7 is five `compilation` values, 5.534, 5.630, 5.784, 6.161, 6.212 ms. Span 11.7%,
over the 10% line. The median is 5.8 ms, so the under-2 ms retake does not apply. The row is
kept. Checksum `3328139` on every JVM and every DuckDB process. The other five checksums
matched the same way: sum `1663923758546`, countByKey `14142272`, reduceByKey `1663934572679`,
join `33281390`, sort `518322534674927`.

### Against the #233 table

That table is unpinned, at `2b7b01e`. This one is pinned, at `b704c3c`. Both facts move a
ratio. Pinning is the protocol change. #234 is in the binary.

- filter + count: 0.732 ms, 1.39x ahead, to 0.691 ms, 1.34x ahead. DuckDB moved from 1.020 ms
  to 0.929 ms. The Scarlet row is the rounds-200 retake.
- filter + sum: no kept row, to 0.718 ms, 1.38x ahead. Five JVMs, span 8.5%.
- filter + countByKey: 1.542 ms, 1.02x behind, to 2.945 ms, 1.95x behind.
- filter + reduceByKey: 1.707 ms, 1.20x ahead, to 2.694 ms, 1.33x behind. Span 27.2%.
- filter + join: 5.360 ms, 1.29x behind, to 5.062 ms, 1.22x behind. Span 3.1%.
- filter + sort: 35.568 ms, 1.97x ahead, to 34.527 ms, 2.04x ahead. Span 6.1%.

Run as in Setup, with `CROSS_WARMUP=80` and `CROSS_ROUNDS=30` as stated for this table. Deltas:
`taskset -c 0,2,4,6` on both engines, DuckDB `--warmup 5 --rounds 30`, Scarlet `java -Xmx10g`
instead of sbt.

```shell
ssh nixos 'LD_LIBRARY_PATH=$(ls -d /nix/store/*gcc-15.3.0-lib/lib | head -1) \
  taskset -c 0,2,4,6 ~/ce/bin/python ~/cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30'

ssh nixos 'cd ~/scarlet && CP=$(cat ~/cp.txt) && \
  CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-count CROSS_WARMUP=80 CROSS_ROUNDS=30 \
  taskset -c 0,2,4,6 java -Xmx10g -cp "$CP" \
  com.ewoodbury.scarlet.execution.CrossEngineBench'
```

The DuckDB command ran five times at 1e6 and five times at 1e7 (`--rows 10000000`). The Scarlet
command ran five times per query at rounds 30, both sizes. Filter + count at 1e6 then ran nine
times at `CROSS_ROUNDS=100` (dropped) and nine times at `CROSS_ROUNDS=200` (the row). `~/cp.txt`
is the `b704c3c` test classpath.

## 2026-11-15 — devbox, steady window

Previous scoreboard, #233. The table stops at `2b7b01e`. Later kernels are not in it: #234
changed `HashJoin`, `Selection`, `JoinWrite`, and `Validity`, and added `Int32Select` and
`SlotFold`. Not the decision table. The difference from this run to the 2026-11-16 pinned table is
`taskset -c 0,2,4,6`. Same machine as the warmup-5 section below.

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
- filter + sum: no kept row. The warmup-5 p50 was 0.888 ms, 1.23x ahead. The readings sit below
  that p50 and stay ahead of DuckDB.
- filter + countByKey: 3.112 ms, 1.90x behind, to 1.542 ms, 1.02x behind.
- filter + reduceByKey: 4.082 ms, 2.00x behind, to 1.707 ms, 1.20x ahead.
- filter + join: 6.120 ms, 1.47x behind, to 5.360 ms, 1.29x behind.
- filter + sort: 36.184 ms, 1.92x ahead, to 35.568 ms, 1.97x ahead.

Run as in Setup, with `CROSS_WARMUP=80` and `CROSS_ROUNDS=30` as stated for this table. On
`nixos`, DuckDB used `~/ce/bin/python` with `--warmup 5 --rounds 30` and Scarlet used
`sbt -batch -J-Xmx8g`, both unpinned. DuckDB ran five times. Scarlet ran five times per five,
three fives in all, from a detached checkout of `2b7b01e`. `Test / fork` is false, so each boot
is the bench JVM.

## 2026-11-15 — devbox, warmup 5

Earlier decision table. Commit `8958ac9`. Same machine and the same paired protocol as the
2026-11-14 second run. Not the decision table.

- Machine: `nixos`, x86_64, 32 cores, 62 GB, OpenJDK 21.0.13, sbt 2. The CPU governor is
  `powersave`.
- Protocol: 1e6 rows, 4 threads, 2 filters, no nulls, warmup 5, rounds 30. DuckDB 2.0.0, one
  in-memory run. Scarlet is the upper median of five fresh JVMs. Spill is on in the Scarlet harness.
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

Protocol: 1e6 rows, 4 threads, two filters, no nulls, warmup 5, 30 rounds. Scarlet is the upper
median of five fresh JVMs. DuckDB is one in-memory run. The join median is 6.120 ms and sits inside
5.9–7.1 ms. The five p50s are 5.736, 5.979, 6.120, 7.045, and 10.715 ms. The 10.715 ms sample is
the JVM that compiled three columnar sources before the rounds. Two later fives of the same
Scarlet invocation (warmup 5, rounds 30, `sbt -J-Xmx8g` on `nixos`), after this harness had
already run, median'd at 10.359 ms (span 6.337–10.884) and 10.997 ms (span 8.112–11.223).

Run as in Setup, with `CROSS_WARMUP=5` and `CROSS_ROUNDS=30` as stated for this table. On
`nixos`, DuckDB was one in-memory run (`--warmup 5 --rounds 30`) and Scarlet used
`sbt -batch -J-Xmx8g`. Scarlet ran five times. Each log booted sbt from `welcome to sbt`, and
the table is the upper median of those five p50s.

### Stability

The join publishes above are 6.120 ms, then 10.359 ms, then 10.997 ms for that same warmup-5
Scarlet invocation. 10.997 / 6.120 is 1.80×. That is larger than the kernel gaps this
scoreboard is trying to see. The devbox governor was `powersave`. This container is aarch64,
4 cores, Vendor ID Apple, OpenJDK 21.0.13. `/sys/devices/system/cpu/cpu0/cpufreq` is absent and
`sudo` is absent, so the governor was left unset.

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
- Publish the upper median of the kept `steady_ms` values, index `n/2`. Drop the set when
  `(max - min) / median` is above 0.10, or when fewer than three JVMs remain.
- If `scaling_governor` reads `powersave` and `sudo -n true` succeeds, set `performance` for
  the run and record it. Otherwise record the governor and leave it.
- Pin both engines with `taskset -c 0,2,4,6`, four threads on four physical cores. The
  decision table does this. The 2026-11-15 steady window did not. An action under 2 ms whose
  five-JVM span is over 10% is retaken at more rounds until the kept span is inside 10%.
  Count and `reduceByKey` stay in the table when the rule fires on a one-millisecond action;
  the span is published beside the number.

The Scarlet milliseconds in this section are warmup-5 full-window p50s. They are not the
decision table. The pinned section is the one that applies this rule. The checksums stay on
the formulas in that table.

## 2026-11-14 — devbox, second run

Earlier decision table. Commit `ffca249`. Same machine as the 2026-11-13 section, paired protocol.
Not the decision table.

- Machine: `nixos`, x86_64, 32 cores, 62 GB, OpenJDK 21.0.13, sbt 2.
- Protocol: 1e6 rows, 4 threads, 2 filters, no nulls, warmup 5, rounds 30. DuckDB 2.0.0, one
  in-memory run. Scarlet is the upper median of five fresh JVMs. Spill is on in the Scarlet harness.
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

Run as in Setup, with `CROSS_WARMUP=5` and `CROSS_ROUNDS=30` as stated for this table. On
`nixos`, DuckDB was one in-memory run (`--warmup 5 --rounds 30`) and Scarlet used
`sbt -batch -J-Xmx8g`. Scarlet ran five times. Each log booted sbt from `welcome to sbt`, and
the table is the upper median of those five p50s.

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
Run as in Setup, with `CROSS_WARMUP=3` and `CROSS_ROUNDS=10` as stated for this table. On
`nixos`, DuckDB used `--rounds 10` and Scarlet used `sbt -batch -J-Xmx8g`.

## Setup

Canonical reproduction. `CrossEngineBench` defaults are warmup 80, rounds 30, `CROSS_ROWS=1000000`,
`CROSS_FILTERS=2`, `CROSS_NULLS=false`, `CROSS_THREADS=4`, spill on for this harness only. That is
the protocol of the 2026-11-17 pinned table, the 2026-11-16 pinned table, and the 2026-11-15
steady-window table, except the deltas those sections name (pin, heap, `java -cp` vs sbt, and
rounds on the short rows). Current ratios are in "2026-11-17 — devbox, pinned". The 2026-11-07
table below is a one-time Apple M1 Pro run, warmup 3,
measured 10, p50. It is not this command and is not comparable with the container or the devbox.
The 2026-11-13 table used warmup 3, rounds 10 on the devbox.

```shell
# DuckDB. Pass --warmup and --rounds as stated for the table being reproduced.
python3 -m venv /tmp/cross-engine-venv
/tmp/cross-engine-venv/bin/pip install duckdb pandas pyarrow
/tmp/cross-engine-venv/bin/python cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30

# Scarlet: harness defaults, warmup 80, rounds 30
CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-count CROSS_WARMUP=80 CROSS_ROUNDS=30 \
  sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
```

One-time comparison, 2026-11-07. Apple M1 Pro, warmup 3, measured 10. Not the command above.

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
Current ratios are in "2026-11-17 — devbox, pinned".

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
"2026-11-17 — devbox, pinned".

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

The 2-5x gap here is the 2026-11-07 M1 Pro history, not the decision table. Current ratios are in
"2026-11-17 — devbox, pinned". On that M1 run the gap to DuckDB on scans,
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

# Scarlet side: Setup protocol at 10M rows, one query per process, 4 GB heap.
# The 2026-11-07 M1 table and the 2026-11-08 spilled harness used warmup 3, measured 10.
CROSS_ROWS=10000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_QUERY=filter-count \
  CROSS_WARMUP=80 CROSS_ROUNDS=30 \
  sbt -J-Xmx4g "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.CrossEngineBench"
```

`CROSS_ROWS` defaults to 1000000, `CROSS_FILTERS` to 2 (`1` is the key filter only), and
`CROSS_NULLS` to false. Run Scarlet as in Setup for the harness defaults, warmup 80 and rounds 30,
so a steady tail is visible. The 10M command above is that protocol at `CROSS_ROWS=10000000`.
It is not the 2026-11-07 M1 protocol (warmup 3, measured 10) and not the 2026-11-13 protocol
(warmup 3, rounds 10). `steady_ms` is the upper median of
the last 10 rounds, index `n/2` after sorting. Publish it when `steady` is `ok` or
`compilation`. Drop a JVM tagged `machine` (the two halves of those last 10 differ by more
than 10%). Drop the set when fewer than three JVMs remain, or when `(max - min) / median` of
the kept values is above 0.10. The decision table pins the process with `taskset -c 0,2,4,6`
and records the governor (`powersave` on the devbox; `sudo -n true` failed, so it was left).
Set `CROSS_QUERY`
to one of `filter-count`, `filter-sum`, `filter-groupbykey`, `filter-reducebykey`, `filter-join`,
`filter-joinsum`, `filter-sort`, `filter-countbykey`, `filter-distinct`, `filter-topn`,
`filter-twogroup`, or `filter-keymod`. Omit it to run all twelve in that process, dropping each
query's input before the next. `filter-joinsum` sums `left * 31 + right` over the value columns
the int inner join writes. It lowers to the same hash join as `filter-join`. `filter-distinct`
is `count(DISTINCT k)` and `filter-twogroup` is `count` plus `sum(v)` grouped by `k`; both lower. `filter-topn` is the sort-key table (`sk`, `v`)
ordered by `v` descending, `sk` ascending, limited to 100. `filter-keymod` groups by `k % 100`.
`filter-keymod` does not lower: `batches`, `fused`, and `arm` are `row`, and the row is one checksum
execution, not a timed p50. `encode_ms` is reported separately and is not subtracted from the
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
