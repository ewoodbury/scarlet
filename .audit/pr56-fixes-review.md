# Focused review — PR #56 follow-up commit

Review ONLY. Do not modify files. Inputs: `.audit/pr56-fixes.diff` (the follow-up commit), plus the files it touches.

The follow-up claims to fix two findings from earlier reviews:
1. The thread-local grouped-sum table was retained on the calling thread forever; `HashAggregate.releaseSumWorkers()` is now called after `GroupedSumKernel.run` and `partialMorsels`, so ranges inside one action still share the table but the action drops it at the end.
2. `GroupTable.release()` cleared only state/seen/sums; it now also clears counts/min/max under their `has*` guards, and the redundant post-compact clear is gone.

Answer concretely:
1. Is `releaseSumWorkers()` actually reached on every exit path, including an exception inside the scan or `compact`? If it is in a `finally`, is the table still usable for subsequent ranges in the same action?
2. Does calling `remove()` break reuse within one action on the calling thread when `parallelism == 1` (single thread handles many ranges) — i.e. does the table get dropped per range instead of per action?
3. Is `release()` now complete for a table with counts/min/max, and does clearing after `grow` or after a partially-filled scan leave any stale state (limits, missing, occupied, slot list)?
4. Does the shared `PairStep.valueKeeps`/`keepValues`/`PassValue` move change any routing behaviour between `ColumnPipeline.sumByKey` and `GroupedSumKernel.run`?
5. Are the two new tests (all-null reuse at parallelism 1; grow-then-reuse at capacity 64) actually exercising the fixed paths, or can they pass with the bug present?
6. Anything else in this delta that regresses correctness, determinism, allocation, or the untouched `PartialCombine`/`HashReduce` behaviour.

Output ONLY:
## PR #56 follow-up
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.
Write '- none' for empty sections.
