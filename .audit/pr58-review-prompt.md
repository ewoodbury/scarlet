Review pull request #58 in this repository (Scala 3, sbt, columnar engine). Review only; do not modify files. Read .audit/pr58.diff and .audit/pr58-desc.txt (repo-relative only).

Context: `HashAggregate.mergeSumPartials` replaces concat + `sumInt64` for grouped sums. It owns one table sized from the widest partial, scans each partial in order choosing dense or sparse per partial, grows only on an absent key, compacts once. `PartialCombine`/`HashReduce.byKey` unchanged; single-morsel pass-through kept. Measured: merge 7.535 -> 0.964 ms at 1 thread, 74.40 -> 0.18 MB; REPLAY 48.041 -> 43.647 ms at 1 thread and 19.014 -> 15.368 at 4.

Focus:
1. Does folding in place preserve every null semantics case: group null then valued, valued then null, null-only group, and a null key, on dense and sparse partials?
2. `ensure` growing only on an absent key: is that correct for every caller, and can it ever loop or overflow a table that is full of existing keys?
3. Sizing from `tableSize(2 * widest partial)`: empty partials, one group, 50k groups, and a later superset. Any spurious grow or OOM path?
4. The dense partial fast path that reuses earlier slots: can a stale slot sequence survive `grow` or `release`, or be reused across batches?
5. Cross-batch separation and ordering: first-seen order per batch, no keys leaking between batches, determinism at fixed parallelism.
6. The single-morsel pass-through and zero-row partial handling: do batch/visit counters match the claims (2 batches / 2 visits), and is the element 126/124 footprint untouched?
7. The study re-point: does it measure the new path, and is REPLAY p50 the right query-level claim?
8. Tests: is the differential test against concat+sumInt64 meaningful, and are the null round-trip and grow-boundary cases real?

Output ONLY:
## PR #58
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.
Write '- none' for empty sections.
