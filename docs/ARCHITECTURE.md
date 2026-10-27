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
  tree for eligible plans; its scaladoc documents selection and fusion.

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

The following 163 accepted physical-plan fixtures report the stated layout and lower to the stated physical shape. All 28 filter/map orderings of lengths 2–4 are covered for int, long, double, and string. Boolean is not in that product.
`Unknown(2)` denotes the two source partitions in the small contract fixtures; aggregate and join
outputs use `Hash(defaultShufflePartitions, RowKeyOrdinals)`. Shape strings are exact.

| Accepted plan shape | Layout | `PhysicalOp.shape` |
|---|---|---|
| Int, long, and double source, including an empty long source | `Columnar` | `Scan` |
| Boolean source, including an empty source, one filter, and a two-filter chain | `Columnar` | `Scan`, `Filter(Scan)`, `Filter(Filter(Scan))` |
| Int, long, and double single filter / map | `Columnar` | `Filter(Scan)` / `Project(Scan)` |
| Int, long, double, and string filter/map runs of 2, 3, or 4 steps | `Columnar` for int/long/double; `Utf8Dict` for string | `Pipeline2(Scan)`, `Pipeline3(Scan)`, `Pipeline4(Scan)` |
| Five- and six-step int, long, double, and string runs | `Columnar` for int/long/double; `Utf8Dict` for string | `Project(Pipeline4(Scan))`, `Pipeline2(Pipeline4(Scan))` |
| String source, scalar filter/map, filter-map run, dictionary equality filter and mapped dictionary result | `Utf8Dict` | `Scan`, `Filter(Scan)`, `Project(Scan)`, `Pipeline2(Scan)`, `Filter(Scan)`, `Project(Filter(Scan))` respectively |
| Int-pair source and row/key/value filters | `Columnar` | `Scan`, `FilterRow(Scan)`, `FilterKey(Scan)`, `FilterValue(Scan)` |
| Empty typed int-pair source | `Columnar` | `Scan` |
| Pair `keys`, `values`, int `mapValues`, mapped key/value columns, filtered int `mapValues` | `Columnar` | `Key(Scan)`, `Value(Scan)`, `MapValue(Scan)`, `Project(Key(Scan))`, `Project(Value(Scan))`, `FilterValue(MapValue(Scan))` |
| Int `reduceByKey`, after a value filter or int `mapValues`, and nested reduction | `Columnar` | `HashAggregate(Exchange(Scan))`, `HashAggregate(Exchange(FilterValue(Scan)))`, `HashAggregate(Exchange(MapValue(Scan)))`, `HashAggregate(Exchange(HashAggregate(Exchange(Scan))))` |
| Automatic or explicit shuffle-hash int join | `Columnar` | `HashJoin(Exchange(Scan), Exchange(Scan))` |
| Explicit broadcast int join. The planner builds the right input. A later pass may build the left. Neither side is exchanged. Order is unspecified | `Columnar` | `BroadcastJoin(Scan, Scan)` |
| Broadcast left outer int join. The planner builds the right input. The left input is the probe side, and the op records no left build. Absence is a probe-side row with no partner: the right value's validity bit is clear. A null value is not absence and stays on the row path | `Columnar` | `BroadcastLeftOuter(Scan, Scan)` |
| Shuffle-hash left outer int join, including an absent hint. The left input is the probe side. Its exchange carries a null key to partition 0. The right exchange drops a null key. Each partition builds one table under the broadcast budget. A null value stays on the row path. Sort-merge, and every right or full outer join, stay on the row path | `Columnar` | `HashLeftOuter(Exchange(Scan), Exchange(Scan))` |
| Join after an int aggregate | `Columnar` | `HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))` |
| Int join key/left-value/right-value filters | `Columnar` | `FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleLeftValue(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleRightValue(HashJoin(Exchange(Scan), Exchange(Scan)))` |

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

`groupedSum` is columnar for an `(Int, Int)` source. A long pair source is not. String
`groupByKey` and string `join` are refused. Also refused, and already listed with the contract
fixtures: `distinct`, `partitionBy`, `sortBy`, `groupByKey`, and joins on any key that is not an
int column. `min` and `max` are not in the API. Nullable long and boolean sources have no
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

These 52 row-path fixture shapes report `partitioning = None`: `mapPartitions`, `repartition`,
`coalesce`, `partitionBy`, `flatMap`, `flatMapValues`, `distinct`, `union`, `groupByKey`, `cogroup`,
`sortBy`, map on join triples, a general tuple filter on join triples, non-Int `mapValues`,
string/double pair projection or pair filters, string/double-key `groupByKey`, `reduceByKey`, and
join, explicit sort-merge joins, left, right, and full outer joins (no outer-join kernel), single-precision
`Float` sources/maps, all-null strings, empty generic sources without a type descriptor, and
element maps that do not preserve the column type.
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

A join lowers when its hint is absent, shuffle-hash, or broadcast. Sort-merge is not implemented
on the columnar path: that hint makes `lower` return `None`, and `partitioning` reports no layout.
The absent hint and shuffle-hash stay the columnar hash join above. Broadcast records both
children and builds the right input. A later cost-based pass may build the left input instead.
The other child is probed in place, so the join itself adds no exchange. Output distribution
follows that probe side. Output columns stay `(key, left, right)`. Multiplicity, values, and
null semantics match the shuffle-hash join. Order is unspecified. The build table is estimated
before it is allocated and must fit in `broadcastBuildBudgetBytes` (64 MiB). That budget covers
the table only. An estimate above it fails the action. The inputs are already evaluated, so the
failure does not fall back to the row path. A probe subtree can still contain its own exchange,
for example an aggregate that feeds the join.

`collect`, `foreach`, `reduce`, `fold`, and `aggregate` are deliberate boxed exits: they consume
columnar results as ordinary Scala values at the action boundary. A collected `reduceByKey` also
decodes its aggregate output to boxed rows. This does not change the columnar layout reported for
the plan itself.

Bounded `take` applies to accepted partition-preserving plans: int, long, double, boolean, and string sources with
their supported element filters or maps; int-pair sources with row/key/value filters, `keys`/`values`,
and int `mapValues`. Boolean take is filter and scan only. It visits those partitions in order and decodes only the remaining requested
prefix. A reduce, join, or other exchange/aggregate shape stays on the row path because its output
requires processing the complete upstream input before the first result is known.

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
Policy (compiler-enforced — `Wart.AsInstanceOf` and `Wart.Any` are errors):

- Casts live only at named erasure boundaries, each with a suppression and a comment explaining
  why the cast is safe: `Operation.fromPlan`, `DistCollection.kvPlan`,
  `StageBuilder`/`StageExecutor`/`JoinExecutor`/`ShuffleHandler` (file-level, documented),
  `Task.BroadcastHashJoinTask`, `DAGScheduler.executePartitions`, `ColumnarPlanner`, and the
  storage implementations (`LocalShuffleService`, `LocalBroadcastService`).
- No new cast sites outside these boundaries without a suppression and a justification.

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

## Ordering guarantees

- `collect`/`take`/`count` preserve partition order and within-partition order.
- `sortBy` produces a globally ordered result: sampling-based range partitioning, a local sort
  per range partition, and concatenation in partition order. There is no driver-side merge.
- `union` concatenates left then right.
- `groupBy`-family outputs group by key equality; record order inside groups follows input order.
- `groupByKey`/`reduceByKey`/`cogroup`/`sortBy` keep one output partition per shuffle partition
  and run as one task per partition, matching joins, `repartition`, `coalesce`, and `partitionBy`.
  A given key of a keyed aggregation lives in exactly one output partition.

## Testing notes

- Tests run sequentially (`Test / parallelExecution := false`): `ScarletConf`,
  `ScarletRuntime`, and `ExecutionService` are process-global; suites mutate them. Parallelism
  returns when global state becomes injected (deferred).
- `ScarletConf` is global mutable state: tests that change it restore defaults in `afterEach`.
- Retry tests set `baseRetryDelayMs = 1L` or they take seconds.
- Test logs are silenced to WARN via `modules/scarlet-tests/src/test/resources/log4j2-test.xml`.

## Known limitations (deferred work)

Roadmap and sequencing are tracked externally; the headline items:

- No cross-branch dedup: reusing a `DistCollection` in two places recomputes it; at most one
  shuffle write per stage.
- Fusion has no cost model or general predicate pushdown. `collect` decodes every output row into a
  `Seq`.
- The columnar path does not implement sort-merge join. That hint stays on the row path. A join
  with no hint, or with shuffle-hash, lowers to the columnar hash join. An explicit broadcast
  hint records both children and builds the right input. A later cost-based pass may build the
  left input. The build table must fit in `broadcastBuildBudgetBytes`. The row-path sort-merge
  join is a hash grouping wearing a sort-merge name; a true typed sort-merge join is deferred.
  A left, right, or full outer join stays on the row path. Its `joinStrategy` hint is stored and
  not consulted; the row executor always shuffle-hashes it.
