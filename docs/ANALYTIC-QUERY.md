# Analytic query: filter, join, group, sum

`filter-joinagg` is the shape added in #269 (`CrossEngineShapes.joinAgg`). Filter both int tables, join on the key, map each match to `(key, leftValue.toLong)`, and `reduceByKey` the sum. Two filters. Left keeps `key % 3 != 0` and `value >= 0`. Right keeps `key % 3 != 0` and `value < 50`. Left keys are `row % 1024` over `CROSS_ROWS`. The right table is `10 * 1024` rows, keys `row % 1024`, values `row % 100`. The group is the join key: 682 keys after the key filter. Grouping by the right value was not the scored plan. That map would keep the join's hash partitioner, `reduceByKey` would skip its shuffle, and the sums would not be one global group.

This session ran on the container, not on `sjc-gpu-01`. Four CPUs, 7.7 GiB, OpenJDK 21.0.13. No core pin. DuckDB 2.0.0.

## What lowers

`ColumnarPlanner.lower` of the aggregate is empty. `partitioning` is empty. `tryRun` returns in a fraction of a millisecond and runs no kernel. The harness then takes the row DAG (`DefaultExecutionService.executePartitions`). `batches`, `fused`, and `arm` are `row`.

The prefixes that do lower, quoted from `PhysicalOp.shape` with `columnarExecution` on and two filters:

```
FilterValue(FilterKey(Scan))
HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))
Project(HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan)))))
```

Each filtered side is that first shape. The join's kind is `Triples`. `Project.kind` copies the child, so the map stays `Triples`. `tryRun` of that project is still empty: `ColumnarInterpreter.project` runs the join, then `PairKernel.mapTriple` throws `ColumnarFallback` because the function returns `(Int, Long)`, not `(Int, (Int, Int))` (`PairKernel.scala` `decline`). `ReduceByKeyLowering` accepts only `Pairs`, `LongPairs`, and `StringPairs`. `Triples` is `None` (`ReduceByKeyLowering.scala`). The aggregate therefore never reaches the project, and the hash join does not run.

`TestCrossEngineShapes` pins the same three results: aggregate `lower` empty, join shape as above, project shape as above, project `tryRun` empty.

## What runs

Four stages. Ids from `StageBuilder.buildStageGraph` on this plan. Conf is the harness conf: 4 threads, batch 8192, `defaultShufflePartitions = 4`, spill on. `enableSortMergeJoin` stays `true` (`ScarletConf.scala`). `broadcastJoinThreshold` stays 1000.

| Stage | Runs | Output |
| --- | --- | --- |
| 0 | left source, `filterKeys`, `filterValues` | `Unknown(2)`, `LazyList` |
| 1 | the same on the right | `Unknown(2)`, `LazyList` |
| 2 | `JoinWideOp`, hint empty | `Hash(4, Seq(0))`, strict list |
| 3 | `map`, then `ReduceByKeyLocal` | same hash, 682 rows |

Stage 2's inputs are shuffle reads of stages 0 and 1. `selectJoinStrategy` saw 333,036 left rows and 3,424 right rows, both above 1000, and picked `SortMerge`. The row executor does not read `spillEnabled`. The sort-merge copies stay on the heap.

Stage 3 exists because a shuffle stage cannot grow. The map keeps hash partitioning (`PartitioningInfo.withoutRangeOrOrdering` drops range and ordering only). `canBypassShuffle` sees hash width 4, so `reduceByKey` does not shuffle. The key did not change. The metadata would have kept the hash even if it had.

## Materialised collections

No column batch is built. The row path forces a full row collection nine times between these operators.

1. Left `filterKeys`. `LazyList`, memoized when the next filter pulls it. The key predicate keeps 666,016 of 1,000,000 rows (`row % 1024`, then `key % 3 != 0`).
2. Left `filterValues`. `LazyList` of 333,036 rows. This is the stage result, and it stays in `stageResults`.
3. Left shuffle write. `partitionByKey` appends into buffers and `toSeq` copies a strict sequence of those 333,036 pairs. The join reads this, not the stage result.
4. Right `filterKeys`. 6,820 rows (`10 * 682`).
5. Right `filterValues`. 3,424 rows. Stage result.
6. Right shuffle write. Strict copy of those 3,424 pairs.
7. `SortMergeJoinTask` yield. Strict list of 1,672,023 `(key, (left, right))` rows. This is what the map reads.
8. The map. `LazyList` of 1,672,023 `(key, leftValue.toLong)` rows, forced in the same task by the reduce.
9. `reduceByKeyLocal`. `groupMapReduce(...).toSeq`, a strict list. 682 rows. The action output.

`NullKeys.drop` on that map list is another `LazyList`. No key is null, so the reduce pull memoizes a second chain of the same 1,672,023 projected rows. That is inside the reduce, not a tenth operator.

Inside each join partition, before the yield, the task also builds a filtered copy, a sorted copy, a `groupBy`, and a map of values per key (`Task.scala` `SortMergeJoinTask`). Those die when the task returns. The retained join output is the yield list. `estimateShuffleSize` only reads `size` on the shuffle lists.

## Phase split

Allocation is thread-allocated bytes around each stage, including its shuffle write. Eight rounds in one forked JVM, after warmup, repeated the same totals to 0.001 MB. The four stages sum to 915.892 MB. The harness middle sample of the whole action, a later pass in another JVM, was 915.975 MB.

| Stage | Rows out | Alloc | Collection |
| --- | ---: | ---: | --- |
| 0 left filter + shuffle | 333,036 | 183.439 MB | `LazyList` |
| 1 right filter + shuffle | 3,424 | 1.722 MB | `LazyList` |
| 2 sort-merge join | 1,672,023 | 279.226 MB | `List` |
| 3 map + local reduce | 682 | 451.505 MB | `List` |

The largest term is stage 3. It allocates 451.505 MB to project 1,672,023 match rows and reduce them to 682 groups. A replay that forced the projection `LazyList` and then called `groupMapReduce` attributed 210.575 MB to the projection and 157.239 MB to the reduce. That replay is not the stage: it does not include the second `LazyList` from `NullKeys.drop`, and 210.575 + 157.239 is not 451.505. The stage total is the number that matches the harness.

Wall time on this host did not separate the three large stages cleanly. The harness window below is tagged `machine`. In the phase JVM, round times for stages 0, 2, and 3 each moved by hundreds of milliseconds. Stage 1 stayed near 2 ms. The medians of those eight rounds were 202.934 ms, 2.290 ms, 203.338 ms, and 230.698 ms. They are not a ranking tighter than "stage 1 is not the cost".

A serial replay of the sort-merge body moved across runs. The cross-product section was 78.129 ms, 136.829 ms, and 250.490 ms on three replays. The stable fact inside that task is the product it retains: 1,672,023 rows and 279.226 MB, which stage 3 then boxes again.

## Ratio

Checksum of `sum(key * 31 + sum)` is `836270016740` on DuckDB and on Scarlet. Same value as the #269 run on `sjc-gpu-01`.

DuckDB p50, warmup 5, rounds 30, 4 threads: 6.659 ms.

Scarlet, one JVM, warmup 30, rounds 20, `CROSS_TIME_ROW=true`, `-Xmx3g`, G1: last-10 upper median 566.484 ms, allocation 915.975 MB, `encode_ms` 24.007 (not subtracted). The two halves of that last 10 were 696.514 ms and 557.904 ms, so `steady` is `machine`. 566.484 / 6.659 is 85.1× on this host.

The published devbox pair was DuckDB 5.630 ms and Scarlet 122.627 ms, 21.8×, same checksum, arm `row`. This container reproduced the checksum, the empty physical plan, and the row arm. It did not reproduce 21.8×. Scarlet's steady median here is several times the devbox figure, and the window is tagged `machine`.

```shell
/tmp/duckdb-venv/bin/python /opt/data/home/cross_engine_duckdb.py \
  --rows 1000000 --threads 4 --warmup 5 --rounds 30 --query filter-joinagg

CROSS_ROWS=1000000 CROSS_FILTERS=2 CROSS_NULLS=false CROSS_THREADS=4 \
  CROSS_QUERY=filter-joinagg CROSS_WARMUP=30 CROSS_ROUNDS=20 CROSS_TIME_ROW=true \
  java -Xmx3g -XX:+UseG1GC -cp "$CP" \
  com.ewoodbury.scarlet.execution.CrossEngineBench
```

The phase split used those stage APIs in a main that is not part of this change. `encode_ms` is a `BatchCodec.intPairs` timing before warmup. The action boxes from `IntPairArrays` and does not use that batch.
