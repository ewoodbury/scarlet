# Negative results

## Sort allocation at 1e7

`SortPhaseBench`, one JVM, 2026-11-17, `SORT_ROWS=10000000 SORT_WARMUP=80 SORT_ROUNDS=30
SORT_THREADS=4 taskset -c 0,1,2,3 java -Xmx4g -XX:+UseG1GC -Xlog:gc`, printed
`steady_ms=456.779` (`steady=machine`), `head_ms=301.328`, `gc_ms=228.000`, `alloc_mb=702.096`,
checksum `518322534674927`, `scan_ms=61.772`, `run_ms=31.102`, `gather_ms=6.991`,
`merge_ms=26.583`, `output_ms=186.085`, `other_ms=151.237`, `runs=2`, `index_passes=3`,
`spill_files=0`, and the pause tally on that log printed `pause_events=395`,
`pause_total_ms=21997.2`, `humongous_pauses=51`, `max_ms=239.148`. The same class at
`SORT_WARMUP=1 SORT_ROUNDS=2` on that pin printed `alloc_mb=702.421` and `steady_ms=658.098`,
and `jfr view --width 220 --truncate beginning allocation-by-site /tmp/sort-short.jfr` printed
`List.prependedAll` 26.48%, `Long.valueOf` 22.56%, `Tuple2.apply` 7.23%, and
`Integer.valueOf` 6.68% on the pair decode and the boxed checksum, `Selection.applySelected`
3.26%, and `RadixPass.sortInts` 0.95% (`RankSplit.sortSelectedDense` 0.95%,
`RankSplit.mergeTwo` 0.95%).

## countByKey at 1e7

`CountByKeySplit`, 2026-11-16, `CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4
CROSS_WARMUP=80 CROSS_ROUNDS=30 taskset -c 0-3 java -Xmx4g`, printed at `CROSS_ROWS=1000000`
`phase=action steady_ms=2.952` (`steady=ok`), `alloc_mb=0.669`, checksum `11147169`, `young=1`,
`stripes=4`, `partials=4`, `dense=4`, and at `CROSS_ROWS=10000000` `phase=action
steady_ms=12.236` (`steady=ok`), `alloc_mb=3.036`, checksum `14142272`, `young=1`, `stripes=4`,
`partials=5`, `dense=5`, with `phase=insert steady_ms=11.266` (`steady=ok`), `phase=merge
steady_ms=0.222`, `phase=exchange steady_ms=0.141`, `phase=encode steady_ms=0.097`. The
`b704c3c` table in `docs/CROSS-ENGINE.md` (`taskset -c 0,2,4,6`, five JVMs, warmup 80, rounds
30) prints 2.945 ms at 1e6 and 40.288 ms at 1e7, checksums `11147169` and `14142272`, 3.91×
behind DuckDB 10.302 ms; `CountByKeySplit` on `taskset -c 0-3` printed 2.952 ms / 0.669 MB and
12.236 ms / 3.036 MB, and the 11.266 ms insert is the phase inside that 12.236 ms action
(`dense=5`, `young=1`).

## Join parallelism

The measurement is `docs/JOIN-PARALLELISM.md`: `JOIN_PHASE_ROWS=10000000 JOIN_PHASE_WARMUP=80
JOIN_PHASE_ROUNDS=30`, five JVMs, `taskset -c 0,1,2,3 java -Xmx2g`, printed a 4-thread action of
17.320 ms with build scan 16.784 ms and an 8-thread action of 15.219 ms (span 14.151–18.783 ms),
and setting `defaultShufflePartitions` to 32 submitted the same 6 tasks at 4 threads and 10 at 8.
