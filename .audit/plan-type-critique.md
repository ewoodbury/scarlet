# Critique request — type coverage plan

Read `.audit/plan-type-coverage.md` (repo-relative only; do not read outside the repo). Design review ONLY: do not modify code or write files.

Context to verify in the code: `LogicalType` has Int32/Int64/Float64/Bool/Utf8Dict and `BatchCodec` can encode/decode all five, but `Plan.SourceDescriptor` has only Unknown/Ints/Doubles/Pairs/Strings, `PhysicalOp.Kind` has no Long/Bool, the public factories are `ofInts`/`ofDoubles`/`ofIntPairs` only, and `SumOf` has instances for Int and Double only. Element pipelines exist for Int32, Float64 and String. `ColumnarPlanner` lowers element chains for those kinds and refuses type-changing maps.

Judge:
1. Is the gap inventory correct, or is Long/Boolean further along than the plan claims (or further behind)?
2. Option C (separate Int64Pipeline plus a tiny BoolPipeline) vs a generic primitive pipeline: which is the right call given the 1200-line cap, the existing duplication between Int32Pipeline and Float64Pipeline, and the risk to measured paths?
3. Long sum semantics: wrapping, addExact, or refuse? What does the row path's `Iterator.sum` do today for Long, and what would users expect?
4. Is Boolean columnar support worth building at all for a "normal queries" baseline, or is a row-path Boolean filter acceptable? If built, what is the minimum surface (filter/map/count only)?
5. Missing pieces in the plan: min/max, distinct, take, nullable Long/Boolean sources, `ofStrings`, join/group-by keys, `partitionBy`. Which belong in this baseline and which are correctly out of scope?
6. Planner witness for `Long` and `Boolean` cells: where exactly does inference live, and what breaks for empty sources and for a first element that is a boxed `java.lang.Long`?
7. Contract matrix design: how should it be structured so a new type cannot be added without rows, and so unsupported combinations assert `None` rather than silently passing?
8. Anything in the plan that would quietly change behaviour for existing Int/Double/String paths.

Output ONLY:

## Plan review — type coverage
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
