# Plan — grouped-sum merge stage (after #56)

## Where the time is now

After #56 the grouped query at 1e7 rows / 1024 groups / 1222 morsels measures roughly:

| stage | 1 thread | notes |
|---|---:|---|
| encode | 2.10 ms | copies partitions into columns |
| partial | 36.70 ms | per-worker reusable table, in-place ranges (#56) |
| **merge** | **8.32 ms** | concat each batch's partials, then `sumInt64` |
| exchange | 0.07 ms | hash partition of 1024-row partials |
| final | 0.03 ms | per-bucket `sumInt64` |

Merge is now the second-largest stage and ~18% of the query. Its allocation is 74 MB at 1 thread.

## Why merge costs this much

`PartialCombine.mergeMorsels` does, per input batch:

1. `gather` splits the partials by morsel count (611 for a 5M-row batch) and calls `ColumnBatches.concat` on each group — a full copy of 611 × 1024 rows ≈ 625,600 rows × 12 B ≈ 7.5 MB per batch.
2. `HashAggregate.sumInt64` on the concatenated batch sizes its table `ColumnHash.tableSize(rows) = 2 × 625,600 = 1.25M` slots. At keys 4 B + sums 8 B + state 1 B + seen 1 B that is ~17.5 MB per batch, two batches ≈ 35 MB.
3. Only 1024 groups exist, so the load factor is 0.08% and the table is thrown away after compacting 1024 rows.

So merge pays a copy and a table sized by rows to re-aggregate 1024 groups.

## Proposal: fold partials directly into a small table

New path in `GroupedSumKernel` (sum-only; `HashReduce.byKey` and `PartialCombine` keep their behaviour):

1. **No concat.** Merge each input batch's partials by calling a package-private `HashAggregate.mergeIntoRange(table, partial, keyOrdinal, valueOrdinal)` on each partial in turn. Each partial is read in place; nothing is copied.
2. **Table sized by groups, not rows.** Start the merge table at `tableSize(firstPartial.length)` (1024 rows → 2048 slots) — the first partial already carries one row per group — with `grow` as the backstop for a partial that covers a superset. This is ~26 KB instead of 17.5 MB.
3. **One table per input batch**, compacted once. Partial order is preserved for first-seen ordering.
4. **Parallelism**: batches merge independently (2 batches in the study); keep one task per batch. Within a batch the fold is sequential, which is what makes it cheap (611 × 1024 adds into an L1-resident table).
5. Exchange and final stages unchanged; the merge still emits one partial per input batch.

Expected: merge 8.32 → ~2–3 ms at 1 thread, allocation 74 MB → ~2 MB; query total ~47 → ~41 ms. 4-thread merge 5.35 → ~1.5 ms.

## Invariants

- Same per-key sums; checksum `49999995000000`.
- Null semantics: partials carry the group/sum validity produced by the partial stage; merging must preserve "group with null sum" and never invent a group.
- Output order for a batch remains first-seen across its partials in morsel order.
- One merge task per input batch, so visit/batch counters change only where a concat batch disappears — update pinned counts in the same PR with the reason.
- No change to `PartialCombine.mergeMorsels`, `HashReduce.byKey`, `ColumnPipeline` shapes, exchange, or final merge.

## Tests

- A batch whose partials all contain the same 1024 groups: sums equal the current merge.
- A batch where later partials add new groups beyond the first partial's rows (forces `grow`); sums equal.
- Null-value partials: a group present with a null sum stays null; a group never present is absent.
- One partial only (no fold), zero partials, and a single-group fixture.
- Determinism across repeated runs at 1 and 4 threads; first-seen order preserved per batch.
- Existing grouped golden tests and the 126/124 element footprint unchanged.

## Risks

- **First-partial sizing undershoots**: a later partial with more groups triggers `grow`; 50k groups means a few doublings, still far below the row-sized table. The grow path now preserves order (fixed in #56).
- **Sequential fold within a batch**: with only 2–4 batches the parallelism is low, but the fold is ~1–2 ms and the exchange/final are already serial per bucket.
- **Counter drift**: concat batches disappear from the merge stage; any test pinning them must move in the same PR.

## Non-goals

No chunked accumulation (rejected in plan v4 for all-distinct memory), no dense path (dropped with #57), no spill, no string keys, no change to `reduceByKey`.

## Questions for reviewers

1. Is sizing the merge table from the first partial correct for the general case, or should it estimate from the total partial rows capped at some multiple of groups?
2. Is a sequential per-batch fold the right parallelism, or should partials be folded in pairs (tree reduction) to use more cores?
3. Does folding partials in place preserve the "group exists with null sum" semantics through `sumInt64`'s sparse scan, or does the merge need to carry the missing flag explicitly?
4. Are there callers of `mergeMorsels` in the sum path that would be left behind, or can the new fold fully replace it for grouped sums?
5. What is the smallest test that catches a lost group when partials are folded instead of concatenated?
