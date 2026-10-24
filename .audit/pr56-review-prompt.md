Review pull request #56 in this repository (Scala 3, sbt, columnar engine). Review only; do not modify files. Read .audit/pr56.diff and .audit/pr56-desc.txt (repo-relative only, do not read outside the repo).

Context: the grouped-sum partial stage was rewritten. Previously each morsel was sliced (copied) and got a fresh GroupTable sized 2 × rows. Now a range descriptor reads the batch's columns in place, and a thread-local GroupTable per worker is reused across morsels, cleared through its occupied slot list after each compact. A sum-only entry point replaces the shared PartialCombine call for sums; PartialCombine and HashReduce.byKey must be untouched. Measured: partial 52.75 -> 36.70 ms at 1 thread, 19.93 -> 11.79 at 4, allocation 390 -> 16.9 MB.

Focus:
1. Correctness of table reuse: is clearing only slots(0 until occupied) sufficient (state, sums, seen, missing, counts, min/max)? Can a previous morsel's group survive into the next partial, or can a stale validity bit leak?
2. Thread-local table lifetime: is one table per worker correct under the scheduler's work stealing, and is it released between actions? Any unbounded growth across calls, or memory retained on the calling thread?
3. Range scan: are start/length correct for tail morsels, zero-row batches, and batches smaller than one morsel? Any off-by-one against columnarBatchSize?
4. The grow order fix: does walking slots(0 until occupied) preserve first-seen order through repeated grows, and is the rehash still correct (no duplicate or lost keys)?
5. Null semantics on all three overloads (plain, keep/map, steps) under reuse: does an all-null-value key still produce a group with a null sum, and does a rejected key stay absent?
6. Visit and batch counts: one task per morsel, one partial per morsel — did the counts stay as claimed?
7. Are PartialCombine and HashReduce.byKey genuinely untouched, with no behavioural change for reduceByKey?
8. The ColumnPipeline call shape: runSum now takes a by-name combinedOf and each call site passes batches as both the argument and inside the closure. Is that clean, and is combinedOf evaluated exactly once on every path, including empty input?
9. The benchmark harness re-plumb: does it still measure the same stage, or did it accidentally measure the new path with a different scope?

Output ONLY:
## PR #56
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.
Write '- none' for empty sections.
