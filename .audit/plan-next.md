# Draft plan: next handful of projects after the columnar-completion queue

Baseline: main `14f1299`, 511 tests, contract suite with 121 accepted + 37 excluded fixtures, six documented mismatches (two since fixed), audit ledger ~12 blocking / ~70 should-fix / ~100 nits.

## Part A — first five fixes

A1. Acceptance must match interpretation (type-changing maps).
`ColumnarPlanner.accepted` uses `lower` + `invalidReason`, which is structural. A map that changes element type lowers to `Project(Scan)` and reports a columnar layout, but `tryRun` falls back when it casts the function. Result: `partitioning` lies. Fix: reject at lower time using the same descriptor mechanism already used for `mapValues`, add contract fixtures for `Int => String`, `Int => Long`, `Int => (Int,Int)`, and assert both `partitioning` and `tryRun` agree for every accepted shape.

A2. Same class of lie for shapes that lower but cannot run (sortBy/distinct-shaped plans).
`partitioning` reports `Some` with `Project(Scan)` while `tryRun` refuses. Fix: make `partitioning` derive from a runnable predicate (lower AND interpretable), not just a lowered tree; contract fixtures assert `None` for these.

A3. Join strategy honesty.
Explicit sort-merge/broadcast hints currently lower to the columnar hash join and silently ignore the hint. Fix: refuse a non-default strategy for the columnar path (row fallback), assert in the contract; implementing SMJ/broadcast is a separate chapter.

A4. Action-level shape for grouped sums.
Fused pair-map `groupedSum` is accepted but has no `PhysicalOp.shape`, so the contract cannot pin it. Fix: expose an action-level shape description (e.g. `GroupedSum(PairStep*(Scan))`) used by the contract suite and by planner docs, without inventing a Plan node.

A5. Metrics without global mutable state.
`ColumnMetrics` is a process-global armed/disarmed object, and `ColumnBatch`'s constructor calls into it, so every batch pays a volatile read forever and concurrent tests can corrupt counts. Fix: introduce an explicit metrics sink on the runtime (default no-op), thread it through batch construction and morsel dispatch, keep the existing test counters working via a test sink. This is the deferred redesign; it should land before more footprint assertions are added.

## Part B — next projects (ordered)

B1. Materialisation measurement (the rebuild gating decision).
Instrument per-operator pass counts, wall time and allocated bytes for a chain like `ofInts.filter.map.map.filter.sum` and a grouped plan, at 1 and 4 threads. Question: how much of the gap is the re-slice/concat between operators versus kernels. Output: numbers in the notes log, then a decision among (a) fuse adjacent columnar operators, (b) stream morsels between operators, (c) leave it.

B2. Scheduler pooling, only if B1 shows thread create/join is visible.
`MorselScheduler` spawns and joins a thread set per call; a grouped sum crosses four calls. Measure first; pool only if the numbers justify it.

B3. Chain-length decision (>4 fused steps).
Six maps cost ~2.5x the fused narrow case. If B1 confirms a second pass plus a written intermediate column, size a bytecode/chain-compiler spike (throwaway, no merge) before committing to a real compiler.

B4. Nit sweep of the audit long tail (~100 items), batched by file, mechanical only.

## Non-goals for this round

String group-by/join, Int64/Bool fused pipelines, multi-column/DataFrame plans, selection vectors, pushdown, spill, network shuffle, cost model, Panama/Arrow/DataFusion. Also not now: implementing SMJ or broadcast joins (A3 only refuses the hint honestly).

## Process

One branch per item off current main, stacked only when a later item needs an earlier one. Gates: make lint, make test-lint, make test. Reviews: glm-5.3-flash and deepseek-v4.1-flash-1017 via OpenRouter, grok-4.7 via the grok CLI, on every PR. No agent or model names in PR text. No merges without the review set.
