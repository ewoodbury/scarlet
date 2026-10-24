Review pull request #57 in this repository (Scala 3, sbt, columnar engine). Review only; do not modify files. Read .audit/pr57.diff and .audit/pr57-desc.txt (repo-relative only).

Context: grouped-sum partial stage got a dense direct-index path alongside the existing hash path. PR #56 introduced in-place range scanning with a per-worker reusable GroupTable (partial 36.70 ms at 1 thread). This PR adds: a Long min/max classification per morsel; a dense long[range] accumulator with two presence bits (group seen vs value seen); ascending emission; steps/MapPair staying on hash; a shared zero-origin key array in compactSpan. Measured: partial 11.677/11.748 ms at 1 thread, 4.088/3.912 at 4, allocation 11.68 MB.

Focus, with path:line evidence:
1. Eligibility: is the Long range computation correct for Int.MinValue/MaxValue and negative keys, and is denseCap derived from a real memory budget? Can an ineligible or out-of-window key reach the dense scan?
2. Two-bit presence: does the plain overload still keep a key whose values are all null (group with null sum), and do filtered/steps keep a fully-rejected key absent? Can a stale bit survive the per-morsel reset?
3. The ArrayIndexOutOfBounds trick: if the implementation relies on a bounds violation to detect an out-of-window key, is that safe and correct, and does it hide real bugs? Judge whether it should be replaced by an explicit check.
4. compactSpan sharing one key array across morsels: is the published ColumnBatch safe from later mutation by the next morsel, given callers and sumInt64/HashExchange may retain it? Is the all-present validity reuse equally safe?
5. Ordering: dense emits ascending, hash emits first-seen — is that documented and tested, and does the merge preserve determinism at fixed parallelism?
6. MapPair/steps routing: is it forced to hash in every path, including partialMorsels and any direct run(steps) call?
7. Reset and growth: does the dense accumulator reset only touched keys, and can range changes reallocate without leaking or mis-indexing?
8. The measured numbers: are they credible given the before/after and the harness scope? Any way the benchmark measures a different path than production?
9. Tests: are the Int-overflow, cap-boundary, negative-offset, MapPair-fallback, dense-vs-hash-equality and reset-leakage cases actually present and meaningful?

Output ONLY:
## PR #57
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.
Write '- none' for empty sections.
