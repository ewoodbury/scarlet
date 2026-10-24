# Review PR #59 — planner split

Review ONLY; do not modify files. Repo-aware review.

Read the diff in `.audit/pr59.diff` and verify against the working tree on branch `planner-split`.

Claim: the declared-apply witness was moved from `ColumnarPlanner.scala` (1185 → 1098 lines) into a new `FunctionWitness.scala` (97 lines) with no behaviour change. Contract suite: 122 accepted, 46 excluded, zero mismatches; 563 tests.

Check:
1. Is every moved body identical (logic, allowBridge flags, bridge detection, suppressions), and are all call sites updated?
2. Visibility: are the helpers that were `private` still effectively private, or did the move widen them to the execution package?
3. Does `FunctionWitness` hold any state or caching it did not before?
4. Does the split leave `ColumnarPlanner` coherent for adding Long/Boolean arms, or does it make them harder?
5. Anything the 217-line diff hides (reformatting, deleted comments, changed imports that alter resolution)?

Output ONLY:
## PR #59
### Blocking
- path:line - problem - why it matters - fix
### Should fix
- path:line - problem - why it matters - fix
### Nits
- path:line - problem
### Verdict
APPROVE / APPROVE WITH FIXES / REQUEST CHANGES - one sentence.
Write '- none' for empty sections.
