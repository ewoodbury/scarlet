# Cleanup inventory

What is still worth not copying. No behavior change in this pass. Rank is risk of leaving
the item times how small the fix is. A primitive loop that differs only by array element type
is specialization, not a merge.

`c11`–`c14` closed the docs-truth contradictions, the mechanical helper merges, the three
1200-line splits, and the dead code that had no caller. This file lists only what is still
true in the tree.

Measurements that already killed a premise are in `docs/NEGATIVE-RESULTS.md`: the 1e7
sort printed `alloc_mb=702.096` (`SORT_ROWS=10000000 SORT_WARMUP=80 SORT_ROUNDS=30
SORT_THREADS=4`, `taskset -c 0,1,2,3`) and `countByKey` at `CROSS_ROWS=10000000` printed
`steady_ms=12.236` (`CROSS_WARMUP=80 CROSS_ROUNDS=30`, `taskset -c 0-3`).

There is no `scalar-*` module. The scalar kernels are `Int32Pipeline`, `Int64Pipeline`,
`Float64Pipeline`, `Utf8Pipeline`, and the int/long/string hash, join, and sort twins under
`scarlet-columnar`.

`make test` does not run `runMain` benches. A move inside those files is not pinned by the
suite. Do not delete them. `TestColumnarContract.excludedCases` is not a skipped test: each
excluded plan is asserted to have no layout. No `ignore`, `pending`, or `cancel` suite was
found. `pending` is a local name: a bit mask in `ColumnHash.scala:58`, an iterator in
`RankSplitRuns.scala:282`, a boolean in `SpillStore.scala:591`, and a `Vector` in
`SortScaleBench.scala:698`.

## Closed in this pass

- Contract counts live in the three assertions (`TestColumnarContract.scala`, 214 / 35 / 249)
  and the contract table (`docs/ARCHITECTURE.md`). The arithmetic changelog that still opened
  with "205 accepted, for 240" is gone. Shuffle-write priority is documented once, in
  `docs/DAGScheduler.md`.
- `JoinBuildFixture.tableSize` and `mix` call `ColumnHash`. `ColumnHash` is `private[scarlet]`
  so the bench cannot drift from `TestHashAggregate`.
- `ColumnHash.denseRange` is the mask walk that lived in `Selection` and `FoldedCount`.
- `OrderedRunHeapString` is the string run heap. `StringRankSplit` and `StringRankSplitRuns`
  no longer carry a local copy.
- `AllocProbe` is one test-helper class. Query, join, sort, count-by-key, cross-engine, and
  join-build benches use it. Join-build keeps a 20 µs watch interval; the others use 50 µs.
- `AGENTS.md` matches the machine: local and CI JDK 21, sbt launcher 1.10.11, JDK 17-safe
  APIs. `.github/copilot-instructions.md` points at `AGENTS.md`.
- `docs/CROSS-ENGINE.md` lead table is the 2026-11-17 pinned run at `50591f6`. The `#233`
  table stops at `2b7b01e`. Reproduce uses warmup 80, rounds 30.
- `ColumnarInterpreter.decodePrefix` is deleted. Prefix decode goes through `decodePrefixFor`.
  The outer batch test calls it with `BroadcastLeftOuter`. A clear key bit on an int-pair sort
  stays null there and matches `tryTake` and the row path. `decodeIntPairs` still rejects that bit.

## Ranked remainder

Kept, with the reason. Do not genericise a hot loop to make this list shorter.

- **Public surface with no product caller.** `ScarletRuntime.setForCurrentThread` and
  `clearThreadLocal` (`ScarletRuntime.scala:41`, `:45`): `get` still prefers the thread local.
  `Task.createStageTask`, `createShuffleHashJoinTask`, `createBroadcastHashJoinTask`, and
  `createSortMergeJoinTask` (`Task.scala:20`–`:58`): live code constructs the case classes.
  Public surface / no product caller.

- **`ElementPipeline` still matches the same 28 filter/project shapes three times.**
  `ElementPipeline.scala:28` (int), `:117` (float), `:192` (string). Each arm calls a typed
  unrolled kernel (`Int32Pipeline.FF`, `Float64Pipeline.FF`, `Utf8Pipeline.FF`, …). Longs
  already build an `Int64Pipeline.Step` list (`:93`) and do not share that match. Finding:
  longs diverged. Leave the unrolled `FF` / `PPPP` methods. A merged match would still have
  to call 28 typed methods per element type.

- **Int open-addressed `add` / `locate` are not one table.** The probe
  (`ColumnHash.mix`, then linear) is the same in `HashCount.scala:383`,
  `StringHashCount.scala:379`, `HashReduce.scala:243`, and `StringHashReduce.scala:252`.
  `FoldedCount.scala:898` adds, `:912` locates for insert, and `:925` grows and rehashes.
  HashCount throws when full. HashReduce carries a payload and `op`. Finding: the inventory
  missed the grow policy. Long key tables stay specialized. Hot path.

- **Rank-split spill control stays three files.** `RankSplitRuns.scala` (627),
  `RankSplitLongRuns.scala` (638), `StringRankSplitRuns.scala` (663). The long and string
  files say the run loop, cursor, and merge are duplicated on purpose and the hot loops are
  not generic. Hot path.

- **`HashAggregate.denseRange` is two columns, not a mask walk.**
  `HashAggregate.scala:464`. It is not `ColumnHash.denseRange`.

- **`SortMergeRuns.growWords` copies live bits into `Validity.allocate`.**
  `SortMergeRuns.scala:940`. `Validity.growWords` is `Arrays.copyOf` of the word array.
  Finding: different function.

- **`JoinBuildFixture.doubled` uses `refuse`.** `JoinBuildFixture.scala:466`.
  `ColumnHash.doubled` throws `IllegalStateException` with a join-output message. Bench
  error path.

- **`DistCollectionStudy` keeps its own probe.** `sample` returns elapsed nanos, a full
  table returns `-1` instead of throwing, and the watch parks 200 µs.
  `DistCollectionStudy.scala:236`. Finding: not the `account` probe.

- **`IntTopN` heap is a top-N sift, not a run heap.** `IntTopN.scala:232`. Different.

## Files over 900 lines

Counts are `wc -l`. Structural means one planner, interpreter, or kernel whose private state
is the reason it is one file. Accretion means cases, wrappers, or decode arms appended per
task. None of these is on the 1200-line gate. The three files that were (contract, QueryBench,
JoinBuildBench) split in `c13`.

| Lines | File | Kind | Why it stays |
| ---: | --- | --- | --- |
| 1163 | `ColumnarInterpreter.scala` | Structural | `interpretAt` match over `PhysicalOp` |
| 1153 | `StageBuilder.scala` | Structural | One walk of `Plan` |
| 1136 | `BatchCodec.scala` | Accretion | Encode and decode per column shape |
| 1131 | `Selection.scala` | Accretion | Survivor index plus count and `sumInt32` |
| 1125 | `Int32Pipeline.scala` | Accretion of names | Pattern wrappers; sums share `sumInt32` |
| 1065 | `ColumnarPlanner.scala` | Accretion of gates | Action gates plus `lower` |
| 1049 | `TestColumnFilterRange.scala` | Accretion | Same window cases per type |
| 1040 | `HashAggregate.scala` | Structural | `GroupTable` owns the slots |
| 1026 | `FoldedCount.scala` | Structural | Folded int count table and grow |
| 998 | `TestRightOuterColumnar.scala` | Accretion | Strategy blocks |
| 981 | `TestColumnarActions.scala` | Accretion | `take` / `count` / `sum` / grouped sum |
| 976 | `DenseSum.scala` | Structural | Splitting the class splits the slot arrays |
| 964 | `TestCountByKey.scala` | Accretion | Int, string, long, then builders |
| 950 | `TestSelectionBoundary.scala` | Accretion | Publish versus decode per kind |
| 947 | `SortMergeRuns.scala` | Structural | Cursor and output buffer share the run |
| 939 | `TestFullOuterColumnar.scala` | Accretion | Strategy blocks |
| 933 | `Float64Pipeline.scala` | Accretion of names | Wrappers around `sumFloat` |
| 932 | `ColumnKernel.scala` | Structural | Leave the loops |
| 918 | `MaterialisationStudy.scala` | Accretion | `runMain` harness |
| 914 | `Int64Pipeline.scala` | Structural | Leave until a sixth arity |
| 901 | `HashJoin.scala` | Structural | Leave `Int32Build` with insert and probe |

Next files under the line: `TestColumnarSelection.scala` 894, `TestLeftOuterColumnar.scala`
884, `SpillStore.scala` 848, `TestSelection.scala` 844, `SpillBench.scala` 831,
`ColumnPipeline.scala` 789, `HashExchange.scala` 754, `SortScaleBench.scala` 741,
`TestColumnarContract.scala` 739, `DistCollection.scala` 715, `JoinBench.scala` 713,
`CrossEngineBench.scala` 698, `JoinBuildFixture.scala` 697, `StringRankSplitRuns.scala` 663.

## Specialization, not debt

Do not merge these. The bodies differ by primitive type, and each type has its own suite.

- `sumInt32` / `sumFloat` / the long `sumF`…`sumPPPPP` arms.
- `scatterInt32` / `scatterInt64` / `scatterFloat64` / `scatterBool` in `HashExchange`.
- `LongHashCount.locate` and `LongHashReduce.locate`. They hash `Long` keys.
- `OrderedRunHeap` versus `OrderedRunHeapLong` versus `OrderedRunHeapString`, for the `less`
  compare only.
- `IntArrays`, `LongArrays`, `DoubleArrays`, `BoolArrays`, `StringArrays`.
- `seedUtf8` in `Selection.scala`. It calls `utf8String` and does not share the bit test with
  `seedInt64` / `seedFloat64` / `seedBool`.
- Thin wrappers already on the shared helper: `presentKeys` → `Validity.countSet`;
  `withLeftovers` / `bucketAt` / `sliceAll` → `ColumnBatches`; `sortIndices` →
  `IndexMerge.sort` (int rank split uses `IntIndexSort`).

`private[columnar]` methods with one caller (`tryReuseDirect`, `reuseZero`, `sumReuse`,
`armDirectFailure`, `directTouchedSlots`) are seams, not dead code.

## House rules that still match

- `checkFileLength` fails above 1200. 1200 is allowed (`build.sbt:6`, `AGENTS.md:21`).
- `Test / parallelExecution := false` (`build.sbt:24`).
- `spillEnabled` defaults to false (`ScarletConf.scala:67`).
- `Wart.AsInstanceOf` and `Wart.Any` are errors (`build.sbt:27`). `wartremoverWarnings` is
  `Warts.allBut` of the list at `build.sbt:28`, so those names are neither errors nor warnings.
  There is no `-Werror`. A warning does not fail lint (`AGENTS.md:22`).
- CI runs one sbt process for `checkFileLength`, `scalafixAll --check`, `scalafmtCheck`,
  `test`, and PerfCheck, then `make test-spill`, on Temurin 21
  (`.github/workflows/test-sbt.yaml`).
- `CrossEngineBench` defaults are warmup 80 and rounds 30.
- The row scheduler and execution path are direct style. There is no effect type on that path.
- A tight columnar `while` is allowed (`AGENTS.md`, `build.sbt` excludes `Wart.While`).
