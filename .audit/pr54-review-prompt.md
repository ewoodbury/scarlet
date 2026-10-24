# Review request — PR #54 (trailing-project fold)

Review ONLY. Do not modify source, do not commit, do not post to GitHub.
Inputs in this repo: `.audit/pr54.diff`, `.audit/pr54-desc.txt`.

Context: a trailing int projection feeding `sum`/`count` is now folded into the action fold instead of writing, concatenating and re-slicing a column. Measured: P5 5.000 -> 1.251 ms at 1 thread, 376 -> 126 batches.

Questions to answer concretely, citing path:line:
1. Is the fold semantically identical to write-the-column-then-reduce: overflow, empty source, single element, filter that rejects every row, multiple partitions?
2. Does it cover a trailing project after Pipeline4, a filter inside the chain, a project that is the only element step, and chains based on a `PhysicalOp.Column` (values/keys/mapValues) rather than a Scan?
3. Is the `steps.length <= 4` guard in TrailingProject.scala applied consistently between `sum` and `count`? Does it silently refuse a count chain that the sum path would fold?
4. Are the footprint assertions meaningful, or tautological/redundant?
5. Duplicated logic: survivor encoding `(value << 1) | 1`, the `(MapOp, Project)` matcher, `proj` defined twice — should these be unified?
6. File length headroom in ColumnarPlanner.scala against the 1200-line cap.
7. Any accepted-but-unfolded path where the plan claims columnar but the fold is not taken.

Output ONLY this format, no preamble:

## PR #54
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.

Write `- none` for empty sections.
