# Architecture

Scarlet is a data processing engine inspired by Spark, written in Scala 3. It has a row execution
path and a typed columnar path for selected plans, with type erasure confined to named boundaries.

## Module layout

| Module | Responsibility |
|---|---|
| `scarlet-api` | `DistCollection`: the user-facing lazy API. Builds `Plan` trees, triggers actions. |
| `scarlet-core` | `Plan` ADT, `PlanWide`, `Partition`, `ScarletConf`, `ExecutionService` SPI, IDs. |
| `scarlet-execution` | The compiler and runtime: `StageBuilder`, `Stage`, `Operation`/`WideOp`, `DAGScheduler`, `ExecutionPlanner`, `StageExecutor`, `ShuffleHandler`, `JoinExecutor`, `Task`. |
| `scarlet-runtime` | Execution SPIs (`TaskScheduler`, `ShuffleService`, `Partitioner`, `BroadcastService`) and local in-memory implementations. |
| `scarlet-columnar` | Column batches, vector kernels, hash aggregate and join, and a local hash exchange. |
| `scarlet-tests` | Aggregated ScalaTest suite (sequential by design; see Testing notes). |

Dependency direction: `api -> core`; `execution -> api, core, runtime, columnar`; `runtime -> core`;
`columnar` depends on nothing. The logical layer cannot name physical types — the split is
enforced by the build.

## Logical vs physical

Two vocabularies, separated at the module level:

- **Logical** (`scarlet-core`): `Plan[A]` — an immutable, lazy tree of what the user asked for.
  `DistCollection` transformations only prepend nodes; nothing runs until an action.
- **Physical** (`scarlet-execution`): `StageGraph` — stages, dependencies, shuffle boundaries.
  Narrow operations are fused into stages; wide operations become shuffle stages carrying a
  structured `WideOp` record. `ColumnarPlanner` builds and interprets a separate `PhysicalOp`
  tree for eligible plans; selection and fusion are documented under "Columnar completion
  contract" below, and declared-apply and boolean-pipeline details in `FunctionWitness` and
  `BoolPipeline`.

The rule: `Plan` is legal in the compiler (`StageBuilder` reads it to compile), illegal in the
executor (`StageExecutor`/`ShuffleHandler`/`ExecutionPlanner` dispatch only on `WideOp`).

## The execution path

```text
DistCollection action
  -> ExecutionService (SPI, registered by the execution module)
  -> DefaultExecutionService
       executePartitions:
         bare Plan.Source: return its partitions
         ColumnarPlanner.tryRun: lower and interpret an eligible plan
         otherwise DAGScheduler.executePartitions
       take:
         ColumnarPlanner.tryTake: scan eligible partitions in order and decode their prefixes
         otherwise row path
            -> StageBuilder.buildStageGraph
            -> TopologicalSort
            -> ExecutionPlanner.runStages
                 StageExecutor.getInputPartitions
                 StageExecutor.executeStage
                 ExecutionPlanner.writeShuffleIfNeeded
  -> final partitions, flattened only at the action boundary
```

The columnar planner accepts only supported typed plan shapes when
`ScarletConf.columnarExecution` is enabled. `TestColumnarContract` is the executable inventory:
its `Plan` match has no default case, so adding a logical node requires classifying it. The suite
asserts both `partitioning` and the exact `PhysicalOp.shape`; this table mirrors that contract.
`TestTypeCoverage` is a separate type-by-operation matrix. It does not replace that inventory.
`columnarBatchSize` (default 8192) sets the morsel width.

### Columnar completion contract

The following 202 accepted physical-plan fixtures report the stated layout and lower to the stated physical shape. The excluded list has 35 shapes, for 237 covered in total. All 28 filter/map orderings of lengths 2–4 are covered for int, long, double, and string. Boolean is not in that product.
`Unknown(2)` denotes the two source partitions in the small contract fixtures. Aggregate and join
outputs use `Hash(defaultShufflePartitions, RowKeyOrdinals)`. `sortByIntKey`, `sortByLongKey`, and
`sortByStringKey` use `Range(defaultShufflePartitions, Seq(0))` and `Sorted(Seq(0), Seq(true))`.
That ordering tag is ascending by effective key. A null int or long key orders as zero and stays
null. A null string key orders as `""` and stays null. Shape strings are exact.

| Accepted plan shape | Layout | `PhysicalOp.shape` |
|---|---|---|
| Int, long, and double source, including an empty long source | `Columnar` | `Scan` |
| Boolean source, including an empty source, one filter, and a two-filter chain | `Columnar` | `Scan`, `Filter(Scan)`, `Filter(Filter(Scan))` |
| Int, long, and double single filter / map | `Columnar` | `Filter(Scan)` / `Project(Scan)` |
| Int, long, double, and string filter/map runs of 2, 3, or 4 steps | `Columnar` for int/long/double; `Utf8Dict` for string | `Pipeline2(Scan)`, `Pipeline3(Scan)`, `Pipeline4(Scan)` |
| Five- and six-step int, long, double, and string runs | `Columnar` for int/long/double; `Utf8Dict` for string | `Project(Pipeline4(Scan))`, `Pipeline2(Pipeline4(Scan))` |
| String source, scalar filter/map, filter-map run, dictionary equality filter and mapped dictionary result | `Utf8Dict` | `Scan`, `Filter(Scan)`, `Project(Scan)`, `Pipeline2(Scan)`, `Filter(Scan)`, `Project(Filter(Scan))` respectively |
| Int-pair source and row/key/value filters | `Columnar` | `Scan`, `FilterRow(Scan)`, `FilterKey(Scan)`, `FilterValue(Scan)` |
| `sortByIntKey` of an int-pair scan. Each source partition is sorted by effective key, then rank-merged into `defaultShufflePartitions`. A null key orders as zero and stays null. A null value is retained. Ties keep input order. Output partition sizes differ by at most one, each partition is sorted, and concatenation is globally sorted. Distribution is `Range(defaultShufflePartitions, Seq(0))` and ordering is `Sorted(Seq(0), Seq(true))`, ascending by that effective key. An int-pair filter, including a stack of one, sorts the survivor index and does not gather before the split. A map, mapValues, projection, or other non-filter child stays on the row path. A filter above the sort stays on the row path. Over the budget, with spill on, that selection is materialised and spilled as dense runs | `Columnar` | `SortByIntKey(Scan)`, `SortByIntKey(FilterKey(Scan))`, `SortByIntKey(FilterValue(FilterKey(Scan)))` |
| `sortByLongKey` of a long-pair scan. Rank boundaries match `sortByIntKey`. A null key orders as zero and stays null. A null value is retained. Ties keep input order. Distribution is `Range(defaultShufflePartitions, Seq(0))` and ordering is `Sorted(Seq(0), Seq(true))`. A filter, map, mapValues, projection, or any other non-scan child stays on the row path. A filter above the sort stays on the row path. Over the budget, with spill on, each input partition is one sorted run and the runs are merged `spillFanIn` at a time. The result matches this in-memory split. One input partition is still sorted in memory. At or under the budget, and whenever spill is off, the in-memory split runs and writes nothing | `Columnar` | `SortByLongKey(Scan)` |
| `sortByStringKey` of a string-pair scan. Rank boundaries match `sortByIntKey`. A present key orders by `String.compareTo` (UTF-16 code units). A null key orders as `""` and stays null. Ties keep input order. Distribution is `Range(defaultShufflePartitions, Seq(0))` and ordering is `Sorted(Seq(0), Seq(true))`. A filter, map, mapValues, projection, or any other non-scan child stays on the row path. A filter above the sort stays on the row path. Over the budget, with spill on, each input partition is one sorted run and the runs are merged `spillFanIn` at a time. The result matches this in-memory split. One input partition is still sorted in memory. At or under the budget, and whenever spill is off, the in-memory split runs and writes nothing | `StringPair` | `SortByStringKey(Scan)` |
| Empty typed int-pair source | `Columnar` | `Scan` |
| Long-pair source, including an empty source | `Columnar` | `Scan` |
| Long-pair `keys`, `values`, `mapValues`, row `filter`, `filterKeys`, and `filterValues` | `Columnar` | `Key(Scan)`, `Value(Scan)`, `MapValue(Scan)`, `FilterRow(Scan)`, `FilterKey(Scan)`, `FilterValue(Scan)` |
| Pair `keys`, `values`, int `mapValues`, mapped key/value columns, filtered int `mapValues` | `Columnar` | `Key(Scan)`, `Value(Scan)`, `MapValue(Scan)`, `Project(Key(Scan))`, `Project(Value(Scan))`, `FilterValue(MapValue(Scan))` |
| String-pair source and `keys`, `values`, int `mapValues`, `filterKeys`, `filterValues`. A null key is a value and `""` is distinct | `StringPair`; `Utf8Dict` for `keys`, `Columnar` for `values` | `Scan`, `Key(Scan)`, `Value(Scan)`, `MapValue(Scan)`, `FilterKey(Scan)`, `FilterValue(Scan)` |
| Int `reduceByKey`, after a value filter or int `mapValues`, and nested reduction | `Columnar` | `HashAggregate(Exchange(Scan))`, `HashAggregate(Exchange(FilterValue(Scan)))`, `HashAggregate(Exchange(MapValue(Scan)))`, `HashAggregate(Exchange(HashAggregate(Exchange(Scan))))` |
| Long `reduceByKey` | `Columnar` | `LongHashAggregate(Exchange(Scan))` |
| String `reduceByKey`. A null key is dropped and `""` is kept. The exchange hashes each dictionary string once | `StringPair` | `StringHashAggregate(Exchange(Scan))` |
| Int `groupByKey`. The exchange drops a null key and keeps every value. Value order inside a key is input order. Key order inside a partition is first-seen. One batch is published per shuffle partition, including an empty bucket. `count` is the number of groups. There is no partial combine and no spill. `keys`, `values`, `take`, and a map above the group stay on the row path | `GroupedInt` | `GroupByKey(Exchange(Scan))` |
| String `groupByKey`. The exchange drops a null key and keeps `""`. `"é"` and `"e\u0301"` stay distinct. Value order inside a key is input order. Key order inside a partition is first-seen. One batch is published per shuffle partition, including an empty bucket. `count` is the number of groups. There is no partial combine, no spill, and no sort. `keys`, `values`, `take`, and a map above the group stay on the row path | `GroupedString` | `GroupByKey(Exchange(Scan))` |
| Long `groupByKey` on `(Long, Long)`. The exchange drops a null key and keeps every value. A null value is refused, including beside a null key. Value order inside a key is input order. Key order inside a partition is first-seen. The stored key is the full `Long`. One batch is published per shuffle partition, including an empty bucket. `count` is the number of groups. There is no partial combine and no spill. `keys`, `values`, `take`, and a map above the group stay on the row path | `GroupedLong` | `GroupByKey(Exchange(Scan))` |
| Int `countByKey` on `(Int, V)`. The exchange drops a null key. The value column is never read, so a null value still counts. The stored count is an `Int` and wraps on overflow, like `reduceByKey`. Output layout is `Pairs`. An int-pair filter stack partial-counts a survivor index, including one row, key, or value filter. A map, a scan, and a non-pair child count dense batches. There is no spill path: an input-row estimate over `spillBudgetBytes` before any table exists throws `ClosedKernelRefusal` and the action fails, and that check ignores `spillEnabled`. A string or long key stays on the row path | `Pairs` | `CountByKey(Exchange(Scan))` |
| String `distinct`. One copy of each string, including one null and one empty string. Equal strings share a hash bucket. Order is unspecified | `Utf8Dict` | `Dedupe(Exchange(Scan))` |
| Automatic or explicit shuffle-hash int join | `Columnar` | `HashJoin(Exchange(Scan), Exchange(Scan))` |
| Automatic or explicit shuffle-hash long join. Both sides are exchanged on `Long.hashCode`. A null key is dropped. A null value stays on the row path. There is no spill and no build budget. Broadcast, sort-merge, and outer joins on long keys stay on the row path | `Columnar` | `LongHashJoin(Exchange(Scan), Exchange(Scan))` |
| Automatic or explicit shuffle-hash string join. Both sides are exchanged on `String.hashCode`. A null key is dropped. `""` matches `""`. A null value stays on the row path. The two sides keep independent dictionaries and match by string equality. `"é"` and `"e\u0301"` do not match. A key or value filter on either side, including a stack of one, scatters the survivor index and does not compact. There is no spill and no build budget. Broadcast, sort-merge, and outer joins stay on the row path. Output columns are `(key, left, right)` and the key is the probe row | `StringTriple` | `StringHashJoin(Exchange(Scan), Exchange(Scan))` |
| Explicit sort-merge int join. Both sides are hash-partitioned, sorted inside each partition, and merged. Null keys are dropped before the sort | `Columnar` | `SortMergeJoin(Exchange(Scan), Exchange(Scan))` |
| Explicit sort-merge left outer int join. Both sides are hash-partitioned. The left exchange carries a null key to partition 0. The right exchange drops a null key. A null key does not match. A preserved null key publishes a clear key bit. Absence is the right value's clear bit. A null value stays on the row path | `Columnar` | `SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))` |
| Explicit sort-merge right outer int join. The right exchange carries a null key. The left exchange drops one. The left-outer walk runs on swapped inputs, then the value columns are swapped back to `(key, left, right)`. Absence is the left value's clear bit. A null value stays on the row path | `Columnar` | `SortMergeRightOuter(Exchange(Scan), Exchange(Scan))` |
| Explicit sort-merge full outer int join. Both exchanges carry a null key to partition 0. A group on only one side is emitted. An empty side emits every row of the other side. Either value's clear bit is absence. A null value stays on the row path | `Columnar` | `SortMergeFullOuter(Exchange(Scan), Exchange(Scan))` |
| Explicit broadcast int join. The planner builds the right input. A later pass may build the left. Neither side is exchanged. Order is unspecified | `Columnar` | `BroadcastJoin(Scan, Scan)` |
| Broadcast left outer int join. The planner builds the right input. The left input is the probe side, and the op records no left build. Absence is a probe-side row with no partner: the right value's validity bit is clear. A null value is not absence and stays on the row path | `Columnar` | `BroadcastLeftOuter(Scan, Scan)` |
| Shuffle-hash left outer int join, including an absent hint. The left input is the probe side. Its exchange carries a null key to partition 0. The right exchange drops a null key. Each partition builds one table under the broadcast budget. A null value stays on the row path | `Columnar` | `HashLeftOuter(Exchange(Scan), Exchange(Scan))` |
| Broadcast right outer int join. The planner builds the left input and probes the right. The left-outer kernel runs, then the value columns are swapped back to `(key, left, right)`. Absence is the left value's clear bit. A null value stays on the row path | `Columnar` | `BroadcastRightOuter(Scan, Scan)` |
| Shuffle-hash right outer int join, including an absent hint. The right input is the probe side. Its exchange carries a null key to partition 0. The left exchange drops a null key. Each partition builds the left bucket, probes, and swaps the value columns. A null value stays on the row path | `Columnar` | `HashRightOuter(Exchange(Scan), Exchange(Scan))` |
| Broadcast full outer int join. The right input is built. The left input is probed in place. Each probe thread keeps its own matched-row bitmap, and the bitmaps are OR'd after the probes join. A clear bit emits that build row with the left value absent. Null build keys are carried outside the table and emitted once. An empty probe still emits every build row. A null value stays on the row path. The bitmaps are not part of `broadcastBuildBudgetBytes` | `Columnar` | `BroadcastFullOuter(Scan, Scan)` |
| Shuffle-hash full outer int join, including an absent hint. Both exchanges carry a null key to partition 0. Each partition keeps one bitmap and emits its own unmatched build rows. There is no cross-partition OR | `Columnar` | `HashFullOuter(Exchange(Scan), Exchange(Scan))` |
| Join after an int aggregate | `Columnar` | `HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))` |
| Int join key/left-value/right-value filters | `Columnar` | `FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleLeftValue(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleRightValue(HashJoin(Exchange(Scan), Exchange(Scan)))` |
| Int join whole-row filter and same-type map | `Columnar` | `FilterTripleRow(HashJoin(Exchange(Scan), Exchange(Scan)))`, `Project(HashJoin(Exchange(Scan), Exchange(Scan)))` |

`sum` and `count` of an int column fold a trailing filter/map chain into the action when the
chain starts at an int scan or at `keys` or `values`, including `mapValues(...).values`. A long
column folds the same way from a long scan, and the long sum wraps. A
five-step int or long chain still lowers to a chunk such as `Project(Pipeline4(Scan))`. The action reads
that base once and applies every step, so the pipeline column and the trailing projection are not
written. `lower` splits a longer run into chunks of at most four steps, then another `Project`,
`Filter`, or `Pipeline` on that chunk. The fold walks those chunks. Examples are
`Project(Pipeline4(Scan))`, `Filter(Pipeline4(Scan))`, `Pipeline2(Pipeline4(Scan))`, and
`Pipeline3(Pipeline4(Scan))`, and the same nesting over `Key` or `Value`. `count` uses the fold
from two steps up, so a short chain that ends in a filter does not
write its column. `collect` still writes the column. A type-changing trailing map is refused and
runs on the row path. Double and string chains still write the pipeline column. A string scan
followed only by dictionary literal filters is the count exception: that action reads the code
mask and does not write the filtered column. A map, or a filter that is not a literal list, still
writes its column, and count then reads the finished batch lengths.

`take` uses these same physical shapes for partition-preserving plans; it scans in partition order
and decodes only the requested prefix. The contract checks an int `Pipeline2(Scan)` and pair
`FilterValue(Scan)` with `tryTake`. A boolean filter chain uses the same prefix decode.
How `sortByIntKey`, `sortByLongKey`, and `sortByStringKey` handle `take` is stated once, under bounded `take`.

### Boolean support

Boolean columns support filter, count, and take. A filter chain is one `Filter` node per step, in
source order, for any number of filters. Boolean has no arithmetic and no `sum`. A Boolean map is
refused, including `Boolean => Boolean` and a map after a filter. `Function1` has no
`apply$mcZZ$sp`, so `Boolean => Boolean` cannot be distinguished from `Boolean => Int` by declared
signature. Refusing is the honest contract. The planner does not call the function. `partitioning`
is `None`. The row path runs a map on the source once per element, and a map after a filter once
per element the filter keeps.

| Operation | Columnar |
|---|---|
| filter, including a chain | accepted |
| count | accepted, from batch lengths |
| take | accepted, prefix decode |
| map | refused |
| sum | not in the API |

### Type and operation matrix

`TestTypeCoverage` builds one `DistCollection` per cell and compares it with the row path
(`columnarExecution = false`). The cell set is the product of the types and operations. For an
accepted cell, `partitioning`, `tryRun`, `tryCount`, and `tryTake` are defined and match that row
result. `trySumInt`, `trySumLong`, and `trySumDouble` are defined only for an accepted `Int`,
`Long`, or `Double` result, and only the matching sum is defined. A refused map keeps every gate
`None`; `collect` still returns the row result. Boolean and string sources are accepted, and every
sum gate on them is `None`. There is no Boolean or string `sum` in the API. `Sum` in this table is
the source plan, not a separate logical node, so those cells repeat `Source`. `Count` and `Take`
follow a filter. `MapCount` and `MapTake` follow a same-type map and use that map's gates.

Int, Long, Double, and String same-type maps are accepted. A Boolean same-type map is refused, and
so are Boolean `MapCount` and `MapTake`. `Function1` has no `apply$mcZZ$sp`, so `Boolean =>
Boolean` cannot be distinguished from `Boolean => Int` by declared signature. Refusing is the
honest contract.

The long rows are `1, 2, 3, 4` and sum to 10, so the matrix cannot show overflow. Wrapping is
covered by `TestLongColumnar` (`wrap Long.MaxValue + 1 and sum an empty collection to 0`).
`TestTypeCoverage` asserts the same two-element sum outside the matrix: both paths return
`Long.MinValue`.

| Type | Source | Filter | Map, same type | Map, other type | Count | MapCount | Sum | Take | MapTake |
|---|---|---|---|---|---|---|---|---|---|
| Int | yes | yes | yes | no (`Int => Long`) | yes | yes | yes, as `Long` | yes | yes |
| Long | yes | yes | yes | no (`Long => Int`) | yes | yes | yes, wrapping | yes | yes |
| Double | yes | yes | yes | no (`Double => Int`) | yes | yes | yes | yes | yes |
| Boolean | yes | yes | no | no | yes | no | no | yes | no |
| String | yes | yes | yes | no (`String => Int`) | yes | yes | no | yes | yes |

`DistCollection.ofStrings` stores a `StringArrays` copy of each range and records
`SourceDescriptor.Strings`. An empty array is therefore columnar. `DistCollection.apply` on an
`Iterable[String]` keeps `SourceDescriptor.Unknown`: a non-empty iterable is classified from its
first string, and an empty iterable has no witness and stays on the row path.

`groupedSum` is columnar for an `(Int, Int)` source. A `(Long, Long)` scan, `reduceByKey` on
that scan, an inner shuffle-hash join of that scan, and `sortByLongKey` of that scan are columnar.
`groupedSum` of a long pair stays on the row path. Broadcast, sort-merge, and outer joins on long
keys stay on the row path. A long sort spills over the budget the same way an int sort does.
String `reduceByKey` is columnar: `StringHashAggregate(Exchange(Scan))` drops a null key, keeps
`""`, and hashes each dictionary string once. Int `groupByKey` is columnar:
`GroupByKey(Exchange(Scan))`, layout `GroupedInt`, and `count` is the number of groups. String
`groupByKey` is columnar the same way, layout `GroupedString`. Long `groupByKey` is columnar the same way, layout `GroupedLong`. Int `countByKey` is columnar as `CountByKey(Exchange(Scan))`, layout `Pairs`: it counts rows per key and never reads the value column, so a null value counts and a null key is dropped. Double `groupByKey` stays refused.
An inner shuffle-hash string join lowers to `StringHashJoin(Exchange(Scan), Exchange(Scan))`.
`sortByStringKey` of a string-pair scan is columnar. Over the budget, with spill on, it spills the
same way an int sort does. A null key orders as `""` and stays null.
The remaining exclusions are listed with the contract fixtures below.
`min` and `max` are not in the API. Nullable long and boolean sources have no
descriptor.

Fused `groupedSum` is action-level. It is not a `Plan` node and not a `PhysicalOp`.
`ColumnarPlanner.groupedSumPeel` is the structural peel label of the pair steps that scan walks.
It is not an action shape. A typed int-pair source's label is `GroupedSum(Scan)`. A preceding
row, key, or value filter, or a pair map, wraps that scan in source order with `FilterRow`,
`FilterKey`, `FilterValue`, or `MapPair`. Examples are `GroupedSum(FilterValue(Scan))`,
`GroupedSum(MapPair(Scan))`, and `GroupedSum(MapPair(FilterRow(Scan)))`.
`ColumnPipeline.sumByKey` runs the scan when the fused action can. `lower` returns `None` for
the pair map, and `partitioning` follows that refusal, so the map has no columnar layout.
`tryGroupedSum` still returns the fused sum when the map stays `(Int, Int)`. A map that changes
the pair type, such as `(Int, Int) => (Long, Int)`, is excluded: `partitioning`, `tryRun`, and
`tryGroupedSum` stay on the row path. The structural peel label is still
`GroupedSum(MapPair(Scan))`.

These 35 row-path fixture shapes report `partitioning = None`: `mapPartitions`, `repartition`,
`coalesce`, `partitionBy`, `flatMap`, `flatMapValues`, `cogroup`, `sortBy`, `groupByKey` on
double keys, `mapValues` to string or double, double-pair `keys`, `values`,
`mapValues`, `filterKeys`, and `filterValues`, double-key `reduceByKey` and `join`,
single-precision `Float` sources and maps, all-null strings, an empty int source without
a witness, and element maps that do not preserve the column type (`boolean` to `boolean`, `int`
to `long`, `string`, or a pair, `long` to `int` or `string`, and `double` to `int`).
That is 35 exclusions, and 203 accepted, for 238 covered shapes. String `groupByKey` moved from the excluded list, so 36 - 1 = 35 excluded. Long `groupByKey` is a new accepted fixture, so 199 + 1 = 200 and 234 + 1 = 235. Long `filterKeys` and `filterValues` are new accepted fixtures, so 200 + 2 = 202 and 235 + 2 = 237. Int `countByKey` is a new accepted fixture that adds no exclusion, so 202 + 1 = 203 and 237 + 1 = 238.
Long broadcast, sort-merge, and outer joins stay on the row path. They are covered by execution
tests, not contract fixtures, because the contract does not track the int refusals either.
That last group is an `Int` mapped to `Long`, to a pair, or to `String` (including a filter then
`toString`, and a type-changing map inside a three-step chain), a `Long` mapped to `Int` or
`String`, a `Double` mapped to `Int`, a `Boolean` mapped to `Boolean` or to `Int`, a boolean
filter followed by a map, and a `String` mapped through `length`, plus a pair map from
`(Int, Int)` to `(Long, Int)`. A
type-changing step refuses the whole element chain, so the plan stays on the row path and reports
no columnar layout. Primitive source factories, and `ofStrings`, carry a descriptor, which supplies
the type for empty partitions; when values are present, the planner still requires data inference
to agree with that descriptor. `ofStrings` is what makes an empty string array columnar. An empty
`Iterable[String]` passed to `DistCollection.apply` stays `Unknown` and on the row path. A map of
a pair that stays `(Int, Int)` is also excluded from `lower` and
`tryRun`. `lower` refuses it and `tryGroupedSum` succeeds. The peel label for that map is
`GroupedSum(MapPair(Scan))`, which names the walked steps and is not an action shape. No covered
fixture reports a layout when `lower` refuses the plan. `partitioning` is defined exactly when
the plan is accepted: `lower` returns a physical tree and that tree's layout is valid.

A join of int pairs lowers when its hint is absent, shuffle-hash, broadcast, or sort-merge. An inner
sort-merge of int pairs is accepted: `lower` returns a physical tree and `partitioning` reports
a layout. A left, right, or full outer sort-merge of int pairs is accepted the same way.
Sort-merge on a non-int key is refused. That plan makes
`lower` return `None`, and `partitioning` reports no layout. For int pairs, the absent hint and
shuffle-hash stay the columnar hash join above. A `(Long, Long)` inner join with an absent hint
or an explicit shuffle-hash hint lowers to `LongHashJoin(Exchange, Exchange)`. Both sides are
exchanged on `Long.hashCode`. A null key is dropped on both sides. Output columns are
`(key, left, right)`. There is no spill and no build budget. A broadcast hint, a sort-merge hint,
and every outer join on long keys stay on the row path. Those plans are covered by execution
tests, not contract fixtures, because the contract does not track the int refusals either.
A `(String, Int)` inner join with an absent hint or an explicit shuffle-hash hint lowers to
`StringHashJoin(Exchange, Exchange)`. Both sides are exchanged on `String.hashCode`. A null key
is dropped on both sides. `""` matches `""`. Output columns are `(key, left, right)` and the key
is taken from the probe row. The two sides keep independent dictionaries and match by string
equality. A key or value filter on either side, including a stack of one, scatters the survivor
index and does not compact. There is no spill and no build budget. The layout is `StringTriple`. A null value stays on the row path. A broadcast hint, a
sort-merge hint, and every outer join on string keys stay on the row path.
Broadcast of int pairs records both children and builds the right input.
A later cost-based pass may build the left input instead.
For a broadcast join, the other child is probed in place, so the join itself adds no exchange.
Output distribution follows that probe side. A sort-merge join is
`SortMergeJoin(Exchange, Exchange)`. Outer sort-merge is `SortMergeLeftOuter(Exchange, Exchange)`,
`SortMergeRightOuter(Exchange, Exchange)`, or `SortMergeFullOuter(Exchange, Exchange)`.
Distribution follows the key hash on both sides. A preserved side carries a null key to
partition 0. Absence is a clear bit on the absent value column. A null value stays on the row path.
When columnar execution is off, the row-path sort-merge task still groups by key.
Output columns stay `(key, left, right)`. Multiplicity, values, and null semantics match the
shuffle-hash join. Order is unspecified. The broadcast build table is estimated before it is
allocated and must fit in `broadcastBuildBudgetBytes` (64 MiB). That budget covers the table
only. An estimate above it fails the action. The inputs are already evaluated, so the failure
does not fall back to the row path. A broadcast probe subtree can still contain its own exchange,
for example an aggregate that feeds the join.

`collect`, `foreach`, `reduce`, `fold`, and `aggregate` are deliberate boxed exits: they consume
columnar results as ordinary Scala values at the action boundary. A collected `reduceByKey` also
decodes its aggregate output to boxed rows. This does not change the columnar layout reported for
the plan itself.

Bounded `take` applies to accepted partition-preserving plans: int, long, double, boolean, and string sources with
their supported element filters or maps; int-pair sources with row/key/value filters, `keys`/`values`,
and int `mapValues`; and a long-pair scan. Boolean take is filter and scan only. Long-pair take is
that scan. Take visits the accepted partitions in order and decodes only the remaining requested
prefix. A reduce, join, or other exchange/aggregate shape stays on the row path because its output
requires processing the complete upstream input before the first result is known.
`sortByIntKey` of an int-pair scan, `sortByLongKey` of a long-pair scan, and `sortByStringKey` of
a string-pair scan are not partition-preserving. Take runs the whole sort, then decodes the
requested prefix across the sorted output partitions in order. `take(0)` returns empty without
running. Long-pair decode already keeps a null key and a null value, so the long sort uses that
decoder. String-pair decode already keeps a null key.

On the row path, narrow-only plans compile to a one-stage graph and flow through the same path
as wide plans.

## Compilation: stages and fusion

`StageBuilder` walks the plan recursively:

- Narrow operations chain onto the current stage (`appendOperation`); their operations accumulate
  into a `Vector[Operation]` materialized as fused `Stage` closures — one pass per partition.
- Wide operations cut a stage boundary (`createShuffleStageUnified`), producing a `WideOp`
  (`GroupByKeyWideOp`, `SortByWideOp`, `JoinWideOp`, ...) plus `ShuffleInput` sources and
  `outputPartitioning` metadata.
- Shuffle bypass: when upstream partitioning metadata already matches the wide operation's
  requirement (`Operation.canBypassShuffle`), the operation is appended as a local no-op instead
  of creating a shuffle stage. Example: `partitionBy(4)` then `groupByKey` (default 4) is a local
  group. Bypass reads distribution, not layout: keyed ops require `Distribution.Hash` at the
  target width; `repartition` skips a shuffle for any other distribution with that width.
- `PartitioningInfo` is distribution, ordering, and layout. The row path emits `Layout.BoxedRows`;
  columnar layouts are reported by `ColumnarPlanner.partitioning`. `None` means the row path.
  Sources are `Unknown(width)`. Keyed shuffles are `Hash`. `sortBy` is `Range` plus `Sorted`;
  `map`, `flatMap`, and `mapPartitions` drop both. A repartition or coalesce shuffle is also
  `Unknown`: the writer hashes the element, which is not pair-key hash and not round-robin. A
  bypassed repartition does not move rows, so it keeps the upstream description. Hash and range
  keys are `Seq(0)`, the logical key, not a schema column. `RoundRobin` and `Singleton` are not
  emitted yet.
- Narrow work after a shuffle reads the upstream stage's output in memory (`StageOutput`) — no
  extra shuffle or materialization round-trip.
- The graph is validated (existence, acyclicity, reachability, partitioning sanity,
  side-tag consistency) before execution; see `TestStageGraphValidation` for the enforced
  invariants.

## Shuffle write policy

`ExecutionPlanner` writes a stage's output only when a dependent stage is a shuffle stage, and
the write shape is a value: `ShuffleWriteReason.forDependents` picks, by priority — sortBy
(range partitioned to preserve global order), then partitionBy (key-hashed into its target
count), then repartition/coalesce, then a combine-then-hash write when every dependent is the
same `reduceByKey`, then a generic keyed write. Shuffle IDs come from the `ShuffleService`,
never from stage IDs. `reduceByKey`'s function is treated as associative and commutative.

## Type erasure policy

`Plan[A]` is invariant and stages transport `Partition[_]`, so some casts are unavoidable.
`Wart.AsInstanceOf` and `Wart.Any` are compile errors. A cast in main source is allowed only in
a file that already carries a suppression, next to a comment that says why the cast is safe.
The files below are that set. A new site needs the same suppression and comment.

- **Planner and interpreter.** `ColumnarPlanner`, `ColumnarInterpreter`, `ExecutionPlanner`,
  `PredicatePlan`, `FunctionWitness`.
- **Stage compiler and executor.** `StageBuilder`, `StageExecutor`, `DAGScheduler`, `Task`,
  `Operation`, `DefaultExecutionService`, `JoinExecutor`.
- **Shuffle.** `ShuffleHandler`.
- **Storage.** `LocalShuffleService`, `LocalBroadcastService`.
- **Element kernels.** `ElementPipeline`, `ElementSum`, `ReduceArms`, `PairKernel`, `IntPairCells`.
- **String-pair walk.** `StringPairWalk`, `StringPairPlan`.
- **Codec.** `BatchCodec`, `SourceEncode`.
- **API.** `DistCollection`.

## Runtime SPIs

| SPI | Local impl | Role |
|---|---|---|
| `TaskScheduler[F]` | `LocalTaskScheduler` (thread pool, bounded parallelism) | Runs tasks; applies the `ScarletConf`-derived retry policy at submission time |
| `ShuffleService` | `LocalShuffleService` (in-memory, concurrent map) | Shuffle write/read keyed by `ShuffleId` + `PartitionId` |
| `Partitioner` | `HashPartitioner` (`floorMod` — safe for negative hashes) | Key -> partition assignment |
| `BroadcastService` | `LocalBroadcastService` | Broadcast join support |

`ScarletRuntime` wires these into `RuntimeComponents` (a global holder with a thread-local
override used by tests).

## Failure handling

Tasks retry per `ScarletConf` (`maxTaskRetries`, exponential backoff) inside
`TaskExecutionWrapper`; permanent failure propagates to the caller. Lineage-based recovery was
removed: it could not safely reconstruct arbitrary user functions. Retry is honest and tested
(`TestLocalTaskSchedulerRetry`).

### Columnar fallback

`ColumnarFallback` is a deliberate decline: the planner or interpreter chose not to run a kernel,
and the row path runs instead. A `ClassCastException` from a filter, or from a string map that
declares `String` and returns another value, is also a fallback, and that function may already
have run. An `IllegalStateException` from a physical/logical mismatch or a broadcast build table
above `broadcastBuildBudgetBytes` is not a fallback, and neither is the `IllegalArgumentException`
from a partition-selector `require`. A null value on a left, right, or full outer join, and on a
long inner join, is a `ColumnarFallback`. A null value on a selected row of an int inner join is
also a `ColumnarFallback`; a null on a dropped row is not. A string join does not use that
fallback: a null value never lowers, and a clear live value bit still throws. An over-budget
int `countByKey` throws `ClosedKernelRefusal`. That exception does not extend
`ClassCastException`, so the action fails instead of rerunning on rows.

## Ordering guarantees

- `collect`/`take`/`count` preserve partition order and within-partition order.
- `sortBy` produces a globally ordered result: sampling-based range partitioning, a local sort
  per range partition, and concatenation in partition order. There is no driver-side merge.
- `union` concatenates left then right.
- `groupBy`-family outputs group by key equality; record order inside groups follows input order.
- `groupByKey`/`countByKey`/`reduceByKey`/`cogroup`/`sortBy` keep one output partition per shuffle partition
  and run as one task per partition, matching joins, `repartition`, `coalesce`, and `partitionBy`.
  A given key of a keyed aggregation lives in exactly one output partition.

## Testing notes

- Tests run sequentially (`Test / parallelExecution := false`): `ScarletConf`,
  `ScarletRuntime`, and `ExecutionService` are process-global; suites mutate them. Parallelism
  returns when global state becomes injected (deferred).
- `ScarletConf` is global mutable state: tests that change it restore defaults in `afterEach`.
- Retry tests set `baseRetryDelayMs = 1L` or they take seconds.
- Test logs are silenced to WARN via `modules/scarlet-tests/src/test/resources/log4j2-test.xml`.

## Outer joins

An outer join marks an absent side by clearing that value column's validity bit. Absence is never
a sentinel key or value, so a real `0` and a real null stay distinguishable from "no partner".

The rules that hold for every kind:

- **Key matching.** A null key never matches. A null key on the preserved side is still emitted,
  with the other side absent. A null key on the other side is dropped, because no row can match
  it. Inside the shuffle, the preserved side's exchange carries a null key to partition 0 so the
  row survives to the task; every other exchange keeps dropping null keys.
- **Values.** A null *value* is not absence and the columnar outer path cannot represent it yet.
  A plan whose probe or build value column contains a null stays on the row path, for all three
  kinds.
- **Layout.** The published batch is always `(key, left, right)`. Left outer leaves the right
  column absent, right outer leaves the left column absent, and full outer can leave either or
  both. A right outer over a left-built table swaps the two value columns back before publishing.
- **Full outer tracking.** Broadcast keeps one matched-row bitmap per probe thread and ORs them
  after the probes join; shuffle keeps one bitmap per partition and emits that partition's
  leftovers immediately. A null build key is carried outside the table and emitted once. An empty
  probe still emits every build row.
- **Hints.** Left, right, and full outer on int pairs lower for an absent, shuffle-hash, or
  broadcast hint. Left, right, and full outer sort-merge of int pairs also lower for an explicit
  sort-merge hint. A non-int key, including a long key, stays on the row path. The row executor always shuffle-hashes an
  outer join and ignores the hint.
- **Budget.** `broadcastBuildBudgetBytes` bounds the build table only. The matched-row bitmaps sit
  beside it. When the build contains a null key, the present-key rows are also copied once, and
  that copy is outside the budget as well.
- **Order.** Unspecified, as for inner joins.

## Spill

Grouped sum, an inner broadcast join's build, an int shuffle-hash build bucket, sort-merge joins,
an int-pair `sortByIntKey`, a long-pair `sortByLongKey`, a string-pair `sortByStringKey`, and an
over-budget int, long, or string `reduceByKey` bucket spill when `spillEnabled` is on.
`spillBudgetBytes` is the in-memory trigger. The flag defaults off. `spillMaxFiles` and
`spillMaxBytes` cap one spill store.

A reduce spills per bucket, and the running partial is `O(distinct keys)`, so the bound is one
bucket plus its partial, not the query. The string-key join has no spill at all.

At or under `spillBudgetBytes`, and whenever `spillEnabled` is off, a sort-merge partition stays
in memory. Over the budget, with spill on, each side is written as sorted runs and merged
`spillFanIn` at a time.

An int-pair `sortByIntKey` keeps the physical op `SortByIntKey`. Over the budget, each input
partition is sorted, written as one run, and released. Runs are merged `spillFanIn` at a time,
then scattered by rank, and the result matches the in-memory rank split. One input partition is
still sorted in memory. At or under the budget, and whenever spill is off, that in-memory split
runs and writes nothing. A selection of one or more filters uses the same budget. Over it, the live prefix is
materialised and the dense spill writer runs. There is no selected spill writer.

A long-pair `sortByLongKey` keeps the physical op `SortByLongKey`. It spills the same way: one
sorted run per input partition, merged `spillFanIn` at a time, then scattered by the same rank
boundaries. The long-pair codec is the one the store already writes. One input partition is still
sorted in memory. At or under the budget, and whenever spill is off, the in-memory split runs and
writes nothing. The long-key inner join still has no spill and no build budget.

A string-pair `sortByStringKey` keeps the physical op `SortByStringKey`. It spills the same way:
one sorted run per input partition, merged `spillFanIn` at a time, then scattered by the same rank
boundaries. Each output partition has its own dictionary. The store writes that dictionary, then
the codes. A null key orders as `""` and stays null. One input partition is still sorted in
memory. At or under the budget, and whenever spill is off, the in-memory split runs and writes
nothing.

An int shuffle-hash build bucket over `spillBudgetBytes` is sub-partitioned by the quotient of
the parent width. Each non-empty build piece is one spill file. One piece is joined at a time.
A piece still over the budget throws. At or under the budget, and whenever spill is off, that
bucket stays in memory and writes nothing.

## Closed kernel

The closed kernel is the set of plans that must lower and run columnar on present values. Finishing
it means those plans stay columnar through null values and inputs that do not fit the in-memory
budget, and a decline of one of them is a failure. A plan outside the set is not that promise.
Some of those plans already run columnar. The rest run on the row executor.

In the set today, on present values:

- Int, long, double, and string scans, filters, same-type maps, count, and take.
- Boolean scans, filters, count, and take. A Boolean map is outside the set.
- Sum of int, long, and double.
- Int and long pair row filters, plus key and value filters and same-type mapValues for int, long,
  and string pairs. A whole-row filter on a string pair stays on the row path.
- `reduceByKey` for int, long, and string keys.
- `countByKey` for int keys.
- Inner shuffle-hash join for int, long, and string keys.
- `sortByIntKey`, `sortByLongKey`, and `sortByStringKey`.
- Int outer, broadcast, and sort-merge joins.

Still open inside the set, and therefore not a promise of this commit:

- A stack of one or more slot filters on an int, long, or string pair keeps a survivor index. An
  int-pair row filter does the same. A long-pair row filter and a whole-row string filter do not.
  Single-column filters stay chained column kernels, because an int survivor index is the wrong
  selection for one column.
- `countByKey` over `spillBudgetBytes` throws `ClosedKernelRefusal` and the action fails. It does
  not spill. Long and string `countByKey` are not columnar.
- Long and string inner hash joins do not spill.
- A null value on an outer join, and on a long inner join, is a `ColumnarFallback`. A null on a
  selected row of an int hash join is the same decline. A null on a dropped row stays columnar. A
  string join with a null value does not lower. An inner sort-merge keeps a null value. A clear
  validity bit on an outer join means the partner is absent, so a null value cannot use that bit.
- An int sort of one or more slot filters sorts the survivor index. Over the budget, with spill
  on, that selection is materialised and the dense spill writer runs. A filtered long or string
  sort does not lower.
- `groupByKey`, `distinct`, and `union` already have a columnar path and are not part of the closed
  kernel. `flatMap`, `mapPartitions`, cogroup, untyped `sortBy`, single-precision `Float`,
  double-key wide operators, and type-changing maps stay on the row executor.

`TestClosedKernel` pins the present-value happy path that already lowers and runs: element filters
with count or sum, an int-pair key and value filter, `reduceByKey`, int `countByKey`, inner
shuffle-hash joins, and the typed key sorts. It does not cover the open items.

## Known limitations (deferred work)

Roadmap and sequencing are tracked externally; the headline items:

- No cross-branch dedup: reusing a `DistCollection` in two places recomputes it; at most one
  shuffle write per stage.
- Fusion has no cost model or general predicate pushdown. `collect` decodes every output row into a
  `Seq`.
- The sort-merge operator still materialises its inputs and its result.
- The int shuffle-hash build bucket spills. The long and string shuffle-hash joins, including `longInnerJoin`, do not spill and have no build budget.
- `countByKey` has no spill path. Over the byte budget the action fails instead of rerunning on
  rows or spilling a count table.
- The string-key join does not spill. An input that does not fit stays in memory.
- Int, long, and string `reduceByKey` buckets spill, but the running partial is `O(distinct keys)`,
  so the bound is one bucket plus its partial, not the query. A reduce with more distinct keys than
  fit stays resident no matter how much of the input spills.
