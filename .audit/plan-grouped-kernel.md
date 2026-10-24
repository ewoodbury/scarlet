# Plan — grouped-sum kernel v2 (partial stage)

## Where the time goes (measured, #53 study, 1e7 rows / 1024 groups / 1222 morsels)

| stage | 1 thread | 4 threads | alloc 1t | alloc 4t |
|---|---:|---:|---:|---:|
| encode | 2.10 ms | 2.09 ms | 82 MB | 82 MB |
| **partial** | **52.75 ms** | **19.93 ms** | **390 MB** | **1277 MB** |
| merge | 8.32 ms | 5.35 ms | 74 MB | 74 MB |
| exchange | 0.07 ms | 0.19 ms | 0.08 MB | 0.08 MB |
| final | 0.03 ms | 0.21 ms | 0.08 MB | 0.09 MB |
| total | 62.8 ms | 27.1 ms | 547 MB | 1108 MB |

Partial is 84% of the query at 1 thread, 74% at 4. Exchange and final are noise; encode is 2 ms. Anything that does not attack partial is wasted work.

## Why partial costs this much

1. `ColumnPipeline.runSum` → `PartialCombine.perBatch` → `partialMorsels`: one `HashAggregate.sumInt32` **per morsel**, and each call builds a fresh `GroupTable` sized `ColumnHash.tableSize(rows) = 2 × 8192 = 16384` slots (`HashAggregate.scala:47`, `417`). Per morsel that is keys 64 KB + sums 128 KB + state 16 KB + slots 64 KB ≈ 272 KB, for **1024 groups** out of 8192 rows. 1222 morsels × 272 KB ≈ 333 MB — that is the 390 MB.
2. Every morsel then `compact()`s into fresh arrays and a fresh `ColumnBatch`, so 1222 intermediate batches are allocated and later concatenated (`PartialCombine.mergeMorsels`) and re-hashed.
3. The hash table is sized by **rows**, not by **distinct groups**. With 1024 groups and 8192-row morsels the load factor is ~6%, and the probe sequence still pays a hash mix + linear probe per row.
4. At 4 threads the allocation estimate is 1277 MB for partial alone — the per-worker tables and compaction multiply, and the estimate includes worker TLAB reservations.

## Spike result (measured while this plan was under review)

Throwaway JVM spike, same generator shape (1e7 rows, 8192-row morsels, 1024 groups), warmup 3, n=11, `-Xmx3g`, G1:

| variant | p50 | alloc |
|---|---:|---:|
| A: table sized `2 × rows` per morsel, compact each morsel (current design) | 41.6 ms | 15 MB (arrays reused in the spike; the engine allocates 272 KB per morsel → ~333 MB) |
| B: one growable table per chunk, compact once per chunk (phase 1 below) | 43.2 ms | 0.4 MB |
| B with initial capacity 4096 / 8192 | 43.5 / 42.4 ms | — |
| C: dense direct-index array per chunk | **6.2 ms** | 0.0 MB |

Two conclusions that change the ordering:

1. **Chunking is a memory fix, not a speed fix.** Hash probing at 1024 groups costs the same whether the table is per morsel or per chunk; capacity from 1024 to 16384 barely moves it (43.2 → 42.4 ms).
2. **The only large time win is the dense path** — 6.7× on this workload. It must be phase 1, not an optional phase 3. Its condition (bounded key range) has to be detected cheaply and fall back safely.

## Revised proposal: adaptive dense-first kernel, hash fallback

**Step 0 — classify the key range.** One pass over the batch's present keys computing `min`/`max` (memory-bound, ~2 ms per 1e7 ints). Dense is eligible when `max - min + 1 <= 65536` (512 KB of `Long` per chunk at the limit). Otherwise the hash path is used, which is today's cost minus the per-morsel table churn.

**Step 1 — dense chunk accumulation** (eligible case). Each chunk owns `long[range]` sums plus a presence flag (`byte[range]`). Scan rows, index directly by `key - min`; compact once per chunk, emitting in ascending key order. Deterministic and branch-free; no first-seen append list (measured at +3.8 ms on 1e7 rows, and grouped tests compare maps, not order).

**Step 2 — hash chunk accumulation** (fallback case). One growable table per chunk, initial capacity `nextPow2(4 × distinctEstimate)` where the estimate is `max(distinct seen in the first morsel, 1024)`, grow by doubling. Reuse the table across every morsel of the chunk; compact once.

**Step 3 — merge chunk partials per batch** into one partial per input batch, then the existing exchange and final merge. Unchanged.

Expected outcome on the benchmark workload: partial 52.8 → **~9 ms** at 1 thread (dense scan 4.8 ms + classification 3.7 ms, spike 2), allocation 390 → < 10 MB. The classification pass is the price of adaptation; fusing it into encode removes it (question 3). On a high-cardinality/wide-range workload the fallback lands near today's time (43 ms in the spike vs 41.6 for the current shape) but with the allocation win.

## Why not chunking alone

It buys 390 MB → a few MB and no measurable time. Doing it alone would be a large change for a footprint win. It stays in the design because the dense path needs the chunk accumulator structure anyway, and the fallback should not keep allocating a table per morsel.

## Chunk mechanics

- For each input batch, split its morsels into at most `parallelism` contiguous chunks, so the fold is deterministic and each task owns an index range.
- Each chunk owns one accumulator (dense array or hash table) reused across every morsel in it. No per-morsel table, no per-morsel `compact()`, no intermediate `ColumnBatch`.
- Compact once per chunk; merge chunk partials in chunk order into one partial per input batch.
- `GroupTable.grow` already doubles and rehashes (`HashAggregate.scala:448`) but is dead in production because capacity is seeded at `2 × rows`; the fallback uses it for real.


## Invariants that must not change

- Null key: row dropped, no group. Null value: does not update the sum, group still exists (plain overload semantics; for `steps` overloads a null value is dropped before the steps).
- Sum accumulates in `Long` and does not wrap; merge uses `sumInt64` on the sum column.
- `count` merges by summing the count column, not by counting again.
- Output is one batch per shuffle partition, key-ascending or hash order is not promised — but the **multiset** of groups and sums must be identical, and output order must be **deterministic** for a fixed input (no dependence on thread scheduling). Chunked contiguous ranges give deterministic order at any parallelism; merging chunks in index order keeps it.
- `numPartitions == 0`/empty input still returns `numPartitions` empty batches.
- Public plan shapes, contract fixtures, and `partitioning` decisions are untouched.

## Success criteria (decided before building)

- Partial stage p50 ≤ **12 ms** at 1 thread on the benchmark workload (from 52.8), ≤ **6 ms** at 4 threads (from 19.9). On a wide-range fallback workload, ≤ **45 ms** at 1 thread (today's shape measures ~41.6 in the spike, so no regression) with allocation down by an order of magnitude.
- Partial allocation ≤ **60 MB** at 1 thread (from 390) and the 4-thread estimate must drop by the same order.
- Checksum `49999995000000` unchanged; per-key sums identical to the current kernel for every fixture; null-key and null-value fixtures unchanged.
- No regression on the element path (126/124 footprint and its checksums untouched).
- Deterministic output across repeated runs at 1 and 4 threads. The dense path emits ascending keys where the hash path emits first-seen order; grouped tests compare maps, so this is a documented order change, not a result change.

## Tests

- Existing grouped-sum golden tests, unchanged, must pass.
- New: identical multiset of `(key, sum)` versus the old kernel for a 1e5-row / 64-group fixture with 8192 batch size, at 1 and 4 threads, run 3 times to check determinism.
- New: a high-cardinality fixture (e.g. 200k rows / 50k groups) that forces `grow` at least twice — this is the path the current code never exercises in production.
- New: null-key and null-value fixtures through the chunked path.
- New: a single-morsel batch and a batch smaller than one morsel.
- Footprint assertions: update any grouped batch/visit counts that legitimately change; do not weaken the element-path assertions.

## Risks

- **Determinism**: any work-stealing or hash-order dependence in the chunk merge breaks repeatability. Contiguous chunks + ordered merge is the mitigation.
- **Tail latency**: 2 chunks on 4 threads if batches are few; the current design already has 1222 independent morsels, so chunking reduces parallelism to `parallelism` per batch. With 2 batches × 4 chunks = 8 tasks on 4 threads it is still saturated; with one small batch and 8 threads it would underuse. Acceptable for now; note it.
- **Memory**: one table per chunk is small, but a pathological key space (all distinct) makes each chunk table grow to ~2 × chunk rows, same as today's total. The win is for grouped data; the loss is bounded.
- **Contract**: batch counts change; any test or doc that pins 2444 partial batches must be updated in the same PR with the reason.

## PR split

1. **Adaptive kernel + wiring:** classification pass, dense chunk accumulator with first-seen order, hash fallback with chunk reuse, one partial per batch, tests, footprint updates, before/after benchmark in the body.
2. **Micro-optimisations**, only if (1) leaves something visible: batch-level min/max fused into the encode, bitmap vs byte presence, prefetch. Own PR, own review.

## Questions for reviewers

1. Is chunking by contiguous morsel ranges the right way to keep determinism and still saturate 4 threads, or should the fold be over morsel indices with a deterministic merge order?
2. Is sizing the fallback table from the first morsel's distinct count (×4, next power of two) better than a fixed 1024-slot start that doubles, given the spike showed capacity barely affects probe cost?
3. Is a 65536-value range cap right for the dense path (512 KB of sums per chunk), and should the classification pass be fused into encode rather than run separately?
4. Does reusing `sumInt64` for the chunk merge preserve the null semantics exactly, or does the chunk partial need to carry a "missing" flag per group the way `GroupTable` does?
5. What is the smallest test that would catch a lost update under 4 threads?

## Non-goals

No spill, no string or multi-column keys, no new aggregate functions, no change to shuffle partitioning, no change to the exchange, no Arrow/Panama, no compiler/bytecode generation.
