# Critique request — grouped-sum kernel v2 plan

Read `.audit/plan-grouped-kernel.md` inside the repository (relative path only; do not read outside the repo). This is a design review ONLY: do not modify code, do not write files, do not commit.

Repository context: Scala 3 columnar engine. `HashAggregate.sumInt32` builds a `GroupTable` per morsel sized `2 × rows`; `PartialCombine.perBatch` produces one partial per morsel, concatenates per input batch, then merges; `HashExchange.partitionAll` shuffles; a final `sumInt64` merges each bucket. Measured: partial is 52.75 ms of 62.8 ms at 1 thread for 1e7 rows / 1024 groups / 1222 morsels, allocating ~390 MB.

Judge the plan as a senior reviewer who will have to maintain this:
1. Does the diagnosis match the code? Check `HashAggregate.scala`, `PartialCombine.scala`, `ColumnPipeline.scala`. Is the `2 × rows` table the real cost, or is compaction/`ColumnBatch` allocation the larger share?
2. Is the chunked worker-local accumulator the right fix, or is there a simpler one (e.g. one table per batch, or passing a reusable accumulator into `MorselScheduler.map`)?
3. Determinism: contiguous chunks and ordered merge — is that sufficient, and does it hold when `parallelism` exceeds the number of morsels?
4. The `grow` path: is starting at 1024 slots and doubling safe for the 50k-group case, and does `GroupTable.grow` preserve insertion order in `slots`? (Output order must be deterministic.)
5. Null semantics through the chunk merge — does `sumInt64` preserve "group exists with null sum" and "null value does not update"? Where could it diverge?
6. Are the success criteria (≤25 ms / ≤10 ms partial, ≤60 MB) realistic for this change, too weak, or unachievable without the dense path?
7. What is missing from the test plan, and what is the smallest test that would catch a lost update at 4 threads?
8. Is the PR split right, or should the dense path be part of the first PR?
9. Anything in the plan that is over-engineered, or any place where it quietly changes observable behaviour.

Output ONLY:

## Plan review — grouped kernel v2
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
APPROVE / APPROVE WITH CHANGES / REJECT - one sentence on whether this is the right kernel to build.

Write `- none` for empty sections.
