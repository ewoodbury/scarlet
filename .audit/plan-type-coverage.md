# Plan — type coverage to a working baseline

Goal: any ordinary query on the common scalar types runs on the columnar path or honestly reports the row path. No silent fallbacks, no missing factories, no missing `sum` instances.

## What exists today

| layer | Int | Long | Double | Boolean | String |
|---|---|---|---|---|---|
| `LogicalType` | yes | yes | yes | yes | yes (dict) |
| batch codec | yes | yes | yes | yes | yes |
| source descriptor | `Ints` | **none** | `Doubles` | **none** | `Strings` |
| `PhysicalOp.Kind` | `Ints` | **none** | `Floats` | **none** | `Strings` |
| public factory | `ofInts` | **none** | `ofDoubles` | **none** | via `apply` only |
| element pipeline | `Int32Pipeline` | **none** | `Float64Pipeline` | **none** | `Utf8Pipeline` |
| `sum` instance | Int→Long | **none** | Double→Double | n/a | n/a |
| groupedSum | `(Int, Int)` | **none** | — | — | — |

So a user with `DistCollection[Long]` gets no `sum`, no primitive factory, and no columnar filter/map/count. Boolean has no primitive source at all. That is the gap the plan closes.

## Scope: baseline means

- **Long:** `ofLongs`, `SourceDescriptor.Longs`, `Kind.Longs`, `Int64Pipeline` (filter `Long => Boolean`, map `Long => Long`, 1–4 step chains), `sum: Long` (returning `Long`), `count`, and the same empty/null/overflow behaviour as the row path.
- **Boolean:** `ofBools`, `SourceDescriptor.Bools`, `Kind.Bools`, `BoolPipeline` (filter `Boolean => Boolean`, map `Boolean => Boolean`), `count`. No sum/min/max.
- **Contract:** a type × operation coverage matrix asserting `partitioning` is `Some` exactly for supported shapes and results equal the row path, plus explicit `None` for unsupported ones (type-changing maps, sum over Boolean, string group-by).
- **Double and String:** audit only. Fix any gap found (e.g. `ofStrings` if it is missing) but do not redesign them.

Out of scope, explicitly: `(Long, Long)` groupedSum and joins on Long keys, string group-by/join, Decimal/Date/Timestamp, arrays/maps/structs. Those are separate products.

## Design questions the reviewers must settle

1. **Separate pipelines or one generic primitive pipeline?**
   - Option A: `Int64Pipeline` and `BoolPipeline` as siblings of `Int32Pipeline`/`Float64Pipeline`. Fast to build, follows the existing shape, but adds ~600–900 lines of near-duplicate switch tables against the 1200-line cap.
   - Option B: one `PrimitivePipeline` parameterised by a `ColumnOps[A]` (get, filter, map, encode, decode), with per-type instances. Less duplication, one place for the 4-step pattern, but it is a refactor of the existing two pipelines and risks the measured Int/Double paths.
   - Option C: A for Long (numeric, same shape), B for Boolean (small, no arithmetic).
   Recommendation: **C**, with the boolean pipeline kept deliberately tiny (filters and identity maps only), and no refactor of the Int/Double paths in this project.

2. **Long sum semantics.** `sum` returns `Long`; a sum of `Long` elements can overflow. Options: wrapping (matches `Iterator.sum` for `Long`), `Math.addExact` (throws), or `BigInt` (changes the return type). Recommendation: wrapping, documented, with a test that pins the wrap; an overflow-checked variant is a separate API.

3. **Boolean representation.** `BoolColumn` exists; the pipeline should operate on its array plus validity without repacking. No bit-packing in this project.

4. **Owned arrays.** `ofLongs`/`ofBools` need the same ownership discipline as `IntArrays`/`DoubleArrays`: private constructor, copying factory, package-private `fromOwned`, no public mutable surface.

5. **Planner witness.** `SourceDescriptor` gains `Longs`/`Bools`, `kindOf` maps them, and the witness that infers a type from the first rows must handle `Long` and `Boolean` cells (it currently infers Int/Double/String). A wrong witness for an empty source is the bug #46 fixed; extend the same rule.

## Delivery, in order

1. **Long end to end** (one PR): `LongArrays`, `ofLongs`, descriptor, kind, `Int64Pipeline`, planner lowering and `trySumLong`, `SumOf[Long, Long]`, tests (parity, empty, nulls, wrap, chain lengths 1–4, type-changing map refused), and the coverage-matrix rows for Long.
2. **Boolean end to end** (one PR): `BoolArrays`, `ofBools`, descriptor, kind, `BoolPipeline`, planner lowering, `count` path, tests, matrix rows for Boolean.
3. **Coverage matrix + audit** (one PR): the full type × op table as executable assertions, the Double/String audit, `ofStrings` if absent, and `docs/ARCHITECTURE.md` updated with the supported matrix.

Each PR follows the usual gates: `make lint`, `make test-lint`, `make test`, contract suite green, three-model review, no agent references.

## Measurements

Per type, 1e6 elements, 2 partitions, batch 8192: `filter(_ => true).map(identity).count` and the numeric `sum`, versus the row path, plus allocation. Targets: Long within 1.3× of the Int path (same kernel shape); Boolean filter/count within 2× of Int (smaller element, same loop); no allocation regression versus the row path. Report p50 at 1 and 4 threads, checksums outside the timer.

## Tests that must exist

- Every factory: parity with `DistCollection.apply` on the same data; ownership (mutating the caller's array after construction does not change the collection).
- Every pipeline: 1–4 step chains, filter-only, map-only, filter+map, empty partition, all-null (for reference types), single element.
- `sum` for Long: parity, empty = 0, wrapping case pinned.
- `count` for Boolean: parity.
- Type-changing maps (`Long => Int`, `Boolean => Int`) report `None` and equal the row path, with the mapper called once per element.
- Contract matrix: for each (type, op) pair, `partitioning.isDefined == expected`, and `tryRun`/`tryCount`/`trySum*` agree.
- Null handling: `Long`/`Boolean` sources with nulls if the codec supports nullable columns for them (verify; if not, document that only primitives are supported in this project).

## Risks

- **File-length cap**: two new pipelines will crowd `ColumnarPlanner`; plan to put each pipeline in its own file and keep planner wiring to a few lines.
- **Contract drift**: new kinds must appear in the contract matrix, or the suite silently stops covering them. The matrix is part of PR 1 and 2, not a follow-up.
- **Witness divergence**: empty sources rely on the descriptor; a missing `Longs`/`Bools` case in the witness turns an empty collection into a row-path plan. Test empty sources per type.
- **Sum API**: adding `SumOf[Long, Long]` is source-compatible, but `sum` on `DistCollection[Long]` will now resolve where it previously failed to compile. That is the intent; note it in the PR.

## Questions for reviewers

1. Is option C (separate `Int64Pipeline`, tiny `BoolPipeline`) right, or is the duplication unacceptable enough to justify the generic refactor now?
2. Wrapping `Long` sum: acceptable, or should the baseline refuse and require an explicit overflow mode?
3. Does `count` need any per-type work, or does the existing row-count path already cover new kinds once the source is columnar?
4. What else belongs in the "ordinary query" baseline that this plan misses (min/max, distinct count, `take`)?
5. How should the contract matrix be structured so a new type cannot be added without its rows?
