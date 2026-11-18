# Foundation

The record of the change from a kernel per closed shape to making an unseen plan fast. Every
millisecond below is quoted from the pull-request body named beside it. Those bodies quote a
command run in that session. This file does not re-measure.

## What the six-row board hid

The 2026-11-17 pin at `50591f6` (#258) timed six closed actions against in-memory DuckDB 2.0.0
on `sjc-gpu-01`. At 1e7 rows, four threads, two filters, Scarlet / DuckDB: count 6.188 / 7.018 ms
(1.13× ahead), sum 6.052 / 7.547 (1.25×), countByKey 9.905 / 10.242 (1.03×, alloc 3.119 MB),
reduceByKey 8.476 / 11.100 (1.31×), join count 9.158 / 35.769 (3.91×), sort 139.173 / 687.482
(4.94×). At 1e6, sort is 13.898 / 70.735 ms (5.09× ahead). The six actions are `tryCount`,
`trySumInt`, `countByKey`, `reduceByKey` on a pair scan, `MatchCount` over a join, and
`sortByIntKey`. Source: m4.body.md.

The four composed shapes at `8bfc7ca` (#264), same pin, at 1e7: distinct 62.095 / 9.681 ms
(6.41× behind, checksum `682`), top-N 47.137 / 9.622 (4.90× behind, 162.907 MB), twogroup
89.736 / 11.304 (7.94× behind, 705.531 MB, checksum `1664362168978`), keymod 1203.643 / 12.387
(97.17× behind, arm `row`, checksum `3481589`). Source: x6.body.md.

The analytic query — filter both int sides, join, group by the join key, sum the left value — is
pinned in `docs/ANALYTIC-QUERY.md` (#272). On the devbox at 1e6, DuckDB's upper median was
5.630 ms (span 1.1%) and Scarlet's steady was 122.627 ms on 4 of 5 JVMs (`machine` dropped one,
span 1.7%), checksum `836270016740`, arm `row`: 21.78×. At 1e7 DuckDB was 47.164 ms (span 0.5%);
the log gives no Scarlet time. Source: q1b.log, quoted in #272. That 21.78× is a boxed sort-merge
over `(Int, (Int, Int))`, one nested tuple per match. The join kernel that is 3.91× ahead on its
own never started.

A board that only measures shapes you anticipated tells you nothing about the ones you did not.
The six-row table was accurate and misleading at the same time.

## Kind-based refusal

`ColumnarPlanner.tryRun` returns `Option[Seq[Partition]]`. The caller runs the row DAG on
`None`: the whole plan, or nothing. Admission is a kind test.

An unhinted int join lowers to `HashJoin`, kind `Triples`. A `map` above it keeps kind `Triples`
(`Project.kind = child.kind`). `ReduceByKeyLowering` accepts only `Pairs`, `LongPairs`, and
`StringPairs`, and returns `None` for `Triples`. A join feeding a keyed aggregate therefore never
becomes columnar.

`DefaultExecutionService.executePartitions` then calls `runRow`. With the harness conf,
`enableSortMergeJoin` stays true and both sides sit above `broadcastJoinThreshold` of 1000, so
`selectJoinStrategy` picks sort-merge. The q1b log does not print the strategy. If that run used
this harness, the 122.627 ms includes a full sort of both boxed sides plus the cartesian.

The same gate is why `keymod` (`data.map { case (k, v) => (k % 100, v) }.countByKey`) ran on
rows. A `MapOp` on pairs is not `MapValue`. `lowerElementStep` admitted a project only for an
inner triple, so `countByKey`'s child did not lower and the whole plan was `None`. The
count-by-key kernel that map would have fed is the 1.03×-ahead row (9.905 vs 10.242 ms at 1e7,
#258).

`tryRun` is atomic. A refused tail discards a legal join and redoes the whole plan on rows.

## Three landed fixes

### Aggregate through the join probe (#281, `8818941`)

`filter-joinagg`: filter both int sides, inner join, group by the join key, sum the left value.
`sumJoinLeft` and `sumJoinRight` are the witnesses. The planner emits `JoinProbeSum`, which
updates the existing per-thread `HashAggregate` table during the probe. The group key is the key
column of each probed row. A hash slot only locates the build chain. `JoinWrite` never runs, and
no match columns are produced.

A null measured int adds as zero, the same unbox as the row path. A null on the other side still
matches. A null key is dropped. An over-budget build bucket declines before any write.
`decodeSums` requires both columns present on every published row.

Five JVMs at 1e6, two filters, no nulls, warmup 80, rounds 30, `taskset -c 0,2,4,6`
(fnd1b.body.md). Before, arm `row`, p50 ms: 811.269, 681.015, 889.216, 933.999, 682.546. After,
arm `JoinProbeSumLeft(Exchange(FilterValue(FilterKey(Scan))), Exchange(...))`, p50 ms: 10.909,
9.813, 10.088, 9.395, 8.865. Checksum `836270016740`, identical to the row-path oracle. The
slowest after p50, 10.909 ms, is 11.24× under the 122.627 ms the task opened against (criterion:
5×). #281 does not publish a DuckDB ratio for the new arm.

The first attempt grouped by hash slot rather than by join key: it named a slot by its first
row's key and added every chained value. Two keys sharing a slot would have been summed together.
A review caught it with a BLOCK, plus decode with no validity test, five untested
newly-reachable paths, and the aggregate table released outside a `try/finally`. The rework
carries a collision test as a mandatory requirement: keys 0 and 80 share a home slot in a
16-slot table, and both sums equal the row path.

### Stop writing 33 million rows to add them (#282, `d9151f2`)

`filter-joinsum` at 1e7 summed `left * 31 + right` by writing every match and scanning the
columns back. The write was 33,281,390 rows. q4.body.md split that action on the `7955dca` pin:
write 92.181 ms and 403.929 MB of a published 110.700 ms / 593.811 MB action (1.99× behind
DuckDB 55.522 ms). The sum over columns the write had already produced was 14.961 ms and
0.001 MB. Those phase numbers are separate timings from one JVM on the host that wrote
q4.body.md, and they do not add up to the published 110.700 ms from `sjc-gpu-01`. They are
evidence that the write dominates, not a partition of one run.

Each build chain is now summed once, and a present probe hit adds
`chainSum * 31 + probe * presentCount`. `JoinArms.inner` and `collect` still write triples when
the rows are wanted. A bucket over the spill budget still spills and sums written columns.

Same session (fnd7.body.md),
`CROSS_ROWS=10000000 CROSS_THREADS=4 CROSS_WARMUP=80 CROSS_ROUNDS=30 CROSS_QUERY=filter-joinsum`:
steady 190.370 → 64.154 ms (2.97×), p50 173.871 → 61.562 ms, allocation 593.903 → 190.014 MB.
Checksum `515818008297280` unchanged. Four batches disappeared from the action: the match
columns that are no longer built. `JoinArms.count` had done this for the count case for months;
the general form was one call shape away. #282 does not publish a DuckDB ratio for the new arm.

### A pair map feeds `countByKey` without a decode (#283, `0451a1b`)

`keymod`: `data.map { case (k, v) => (k % 100, v) }.countByKey`, at 1e7. An
`(Int, Int) => (Int, Int)` `MapOp` now lowers to `PhysicalOp.Project` and stays `Kind.Pairs`,
via a declared `apply(Tuple2): Tuple2` witness (the same shape `mapValues` already used for an
unboxed int). The project writes two int columns and calls the function on every row the row
path would. Each column owns its own validity words. A result that is not a pair of ints throws
`ColumnarFallback` before the batch is published. `CountByKey` hashes the project directly.
There is no `decodeIntPairs` between them.

Same JVM (fnd10.body.md): row path 2086.026 ms / 2331.177 MB, with the project 52.741 ms /
360.384 MB, checksum `3481589` both times — 39.6× the row path in that session. 52.741 ms is
5.3× the 9.905 ms `filter-countbykey` reference (#258), so the project is not free yet.

## Approach

1. Fix admission, not the catalogue. A kernel per shape does not scale; an unseen plan has to be
   fast before specialisation is worth adding.
2. Make the gate a schema, not an enum. `Kind` cannot name a projection of `[Int32, Int64]`. The
   replacement is `(ordinal, LogicalType)`, and `Project.kind = child.kind` then stops being
   true.
3. The oracle is the row path's checksum for the same plan. The shape is pinned in the harness
   first, the row-path checksum is taken, and every later comparison is against it.
4. Lowering is not speed. #279 lowered `twogroup` and it still boxed every grouped value.
5. Reuse the structure that is already there. `HashAggregate`'s per-thread table,
   `JoinArms.count`'s probe loop, `ColumnUse.ordinals`, `Morsel` views, `Selection`'s survivor
   index. The wins came from connecting existing pieces.
6. A plan that lowers but is not faster is a failure. A plan that lowers with a different answer
   is worse than one that falls back.

Before the probe-in-place path was timed, BASELINE-ENGINE.md estimated two ceilings. If the
generic path still published match columns, the measured neighbour was `filter-joinsum` at 1.99×
behind DuckDB (110.700 vs 55.522 ms, q4.body.md); confidence in "still about 2× behind, not 22×
behind" was medium. If the probe updated a primitive aggregate and wrote no matches, the
neighbour was the join count (1.077 vs 4.163 ms at 1e6, 9.158 vs 35.769 at 1e7, #258); a fused
Scarlet aggregate at 1e6 "should land in a few milliseconds, in the band of that 5.630 ms", with
**low confidence** until change 1 was timed. #281 then measured 8.865–10.909 ms against the
122.627 ms row oracle. That PR body does not publish a DuckDB ratio.

The 21.78× is the distance from a row sort-merge of boxed tuples to a vectorised aggregate.
Fixing it requires getting off that DAG.

## Still open

- `Kind` still cannot name a `Project` of `[Int32, Int64]`, so a generic
  `join → map → reduceByKey` does not lower (`fnd11`, change 4).
- `tryRun` is still atomic: a sort-merge or broadcast `sumJoinLeft` discards a legal join and
  redoes the whole plan on rows (`fnd13`, change 5).
- `MapKey` in the join tree still refuses `JoinProbeSum` (`fnd14`).
- `keymod` is 39.6× better and still 5.3× off the countByKey floor.
- Distinct (6.41×) and top-N (4.90×) are behind for a different reason: their kernels do too
  much memory traffic for the result they publish. That gap is real, and it is the one that
  makes "write a new kernel" look like the answer.

The `docs/CROSS-ENGINE.md` lead tables are the pre-fix pins (`50591f6`, `8bfc7ca`, `7955dca`).
They still show `keymod` on the row path and `filter-joinsum` at 110.700 ms. The pairs above
are the later same-session measurements.
