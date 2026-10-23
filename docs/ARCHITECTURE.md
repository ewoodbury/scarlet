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
`columnarBatchSize` (default 8192) sets the morsel width.

### Columnar completion contract

The following 122 accepted physical-plan fixtures report the stated layout and lower to the stated physical shape. All 28 filter/map orderings of lengths 2–4 are covered for each scalar type.
`Unknown(2)` denotes the two source partitions in the small contract fixtures; aggregate and join
outputs use `Hash(defaultShufflePartitions, RowKeyOrdinals)`. Shape strings are exact.

| Accepted plan shape | Layout | `PhysicalOp.shape` |
|---|---|---|
| Int and double source | `Columnar` | `Scan` |
| Int and double single filter / map | `Columnar` | `Filter(Scan)` / `Project(Scan)` |
| Int, double, and string filter/map runs of 2, 3, or 4 steps | `Columnar` for int/double; `Utf8Dict` for string | `Pipeline2(Scan)`, `Pipeline3(Scan)`, `Pipeline4(Scan)` |
| Five- and six-step int, double, and string runs | `Columnar` for int/double; `Utf8Dict` for string | `Project(Pipeline4(Scan))`, `Pipeline2(Pipeline4(Scan))` |
| String source, scalar filter/map, filter-map run, dictionary equality filter and mapped dictionary result | `Utf8Dict` | `Scan`, `Filter(Scan)`, `Project(Scan)`, `Pipeline2(Scan)`, `Filter(Scan)`, `Project(Filter(Scan))` respectively |
| Int-pair source and row/key/value filters | `Columnar` | `Scan`, `FilterRow(Scan)`, `FilterKey(Scan)`, `FilterValue(Scan)` |
| Empty typed int-pair source | `Columnar` | `Scan` |
| Pair `keys`, `values`, int `mapValues`, mapped key/value columns, filtered int `mapValues` | `Columnar` | `Key(Scan)`, `Value(Scan)`, `MapValue(Scan)`, `Project(Key(Scan))`, `Project(Value(Scan))`, `FilterValue(MapValue(Scan))` |
| Int `reduceByKey`, after a value filter or int `mapValues`, and nested reduction | `Columnar` | `HashAggregate(Exchange(Scan))`, `HashAggregate(Exchange(FilterValue(Scan)))`, `HashAggregate(Exchange(MapValue(Scan)))`, `HashAggregate(Exchange(HashAggregate(Exchange(Scan))))` |
| Automatic or explicit shuffle-hash int join | `Columnar` | `HashJoin(Exchange(Scan), Exchange(Scan))` |
| Join after an int aggregate | `Columnar` | `HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))` |
| Int join key/left-value/right-value filters | `Columnar` | `FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleLeftValue(HashJoin(Exchange(Scan), Exchange(Scan)))`, `FilterTripleRightValue(HashJoin(Exchange(Scan), Exchange(Scan)))` |

`take` uses these same physical shapes for partition-preserving plans; it scans in partition order
and decodes only the requested prefix. The contract checks an int `Pipeline2(Scan)` and pair
`FilterValue(Scan)` with `tryTake`.

Fused `groupedSum` is action-level: a typed int-pair source with pair filters and/or a pair map
is scanned by `ColumnPipeline.sumByKey`. `partitioning` reports `Columnar`, but `lower` returns
`None` for a pair map and there is no `PhysicalOp.shape` for this fused action today. The contract
checks the fused sum result and pins that absence rather than inventing a physical tree.

These 43 row-path fixture shapes report `partitioning = None`: `mapPartitions`, `repartition`,
`coalesce`, `partitionBy`, `flatMap`, `flatMapValues`, `distinct`, `union`, `groupByKey`, `cogroup`,
`sortBy`, map on join triples, a general tuple filter on join triples, non-Int `mapValues`,
string/double pair projection or pair filters, string/double-key `groupByKey`, `reduceByKey`, and
join, single-precision `Float` sources/maps, all-null strings, empty generic sources without a type
descriptor, an unsupported Boolean source, and element maps that do not preserve the column type.
That last group is an `Int` mapped to `Long`, to a pair, or to `String` (including a filter then
`toString`, and a type-changing map inside a three-step chain), a `Double` mapped to `Int`, and a
`String` mapped through `length`. A type-changing step refuses the whole element chain, so the plan
stays on the row path and reports no columnar layout. Primitive source factories carry a descriptor,
which supplies the type for empty partitions; when values are present, the planner still requires
data inference to agree with that descriptor. A map of a pair is also excluded from `lower` and
`tryRun`; its action-specific `groupedSum` fusion is described above.

Sort-merge and broadcast joins are still columnar hash joins. The contract pins those two shapes,
plus the fused pair map, as the three remaining discrepancies:

- Join strategy is ignored by `ColumnarPlanner.lower`: explicit sort-merge and broadcast joins
  currently report the same columnar `HashJoin(Exchange(Scan), Exchange(Scan))` as shuffle-hash.
- The pair map fused by `groupedSum` reports a columnar layout but has no `PhysicalOp.shape`; the
  action is covered separately and remains action-level.

The grouped-sum pair-map layout is a separate action-level case, not one of the physical-tree
fixtures: `partitioning` reports `Columnar`, `lower` returns `None`, and `tryGroupedSum` succeeds.

`collect`, `foreach`, `reduce`, `fold`, and `aggregate` are deliberate boxed exits: they consume
columnar results as ordinary Scala values at the action boundary. A collected `reduceByKey` also
decodes its aggregate output to boxed rows. This does not change the columnar layout reported for
the plan itself.

Bounded `take` applies to accepted partition-preserving plans: int, double, and string sources with
their supported element filters/maps; int-pair sources with row/key/value filters, `keys`/`values`,
and int `mapValues`. It visits those partitions in order and decodes only the remaining requested
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
- Sort-merge join is a hash grouping wearing a sort-merge name; a true typed sort-merge join is
  deferred.
