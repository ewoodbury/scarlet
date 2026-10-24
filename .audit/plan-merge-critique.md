# Critique request — grouped-sum merge stage plan

Read `.audit/plan-merge-stage.md` inside the repository (repo-relative only; do not read outside the repo). Design review ONLY: do not modify code, do not write files, do not commit.

Context you should verify in the code: `PartialCombine.mergeMorsels` concatenates each input batch's partials and then calls `HashAggregate.sumInt64`, whose table is sized `2 × rows` (`HashAggregate.tableSize`). Grouped sums go through `GroupedSumKernel.merge` → `PartialCombine.mergeMorsels` → `ColumnPipeline` exchange → final `sumInt64`. Measured: merge 8.32 ms of a 47 ms query at 1 thread, 74 MB allocated.

Judge:
1. Does the diagnosis match the code? Is concat or the row-sized table the larger share of the 8.32 ms / 74 MB?
2. Is folding partials directly into a table (no concat) the right fix, or is per-worker accumulation (fewer, larger partials) the better long-term shape given #56's memory constraints?
3. Table sizing from the first partial: correct for a superset later, for a single group, for 50k groups? Or should it use `tableSize(totalPartialRows)` capped at some multiple of groups?
4. Parallelism: sequential per-batch fold with 2 batches on 4+ threads underuses cores. Is a tree/pairwise fold worth it, or is the stage too small to matter?
5. Null semantics: does folding partials through `sumInt64`'s sparse scan preserve "group exists with null sum" and avoid inventing groups? Where could it diverge from concat-then-sum?
6. Determinism and order: does in-place folding preserve first-seen order per batch, and is that order contract worth keeping?
7. What breaks in the counters/tests when the concat batches disappear?
8. Is the expected 8.32 → 2–3 ms credible, and what would falsify it?

Output ONLY:

## Plan review — merge stage
### Blocking
- section:line - problem - why it matters - fix
### Should fix
- section:line - problem - why it matters - fix
### Nits
- section:line - problem
### Answers to the five questions in the plan
1. ...
2. ...
3. ...
4. ...
5. ...
### Verdict
APPROVE / APPROVE WITH CHANGES / REJECT - one sentence.

Write `- none` for empty sections.
