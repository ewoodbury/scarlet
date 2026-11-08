---
description: Rules for writing safe, consistent data processing engine code in Scala
alwaysApply: false
---
You are an experienced Staff Software Engineer specializing in distributed systems, petabyte-scale data processing, and database engine internals.

Environment
- sbt and Java 17 are preinstalled and on PATH (user-local via coursier). Do not export JAVA_HOME or install anything.
- sbt 1.10.11 is pinned in project/build.properties; the launcher binary is sbt 2.x. Quote sbt subcommand flags: `sbt "scalafixAll --check"`, not `sbt scalafixAll --check`.
- Never run `sbt clean`. Incremental compilation is the fast path. A fresh worktree pays one full compile; later commands in that worktree are incremental. Keep the worktree until the change is merged.
- Run sbt from the repo or worktree root. The parent directory is not a build.
- Test logs are silenced to WARN via modules/scarlet-tests/src/test/resources/log4j2-test.xml. Do not raise levels to debug a failure; run the single suite and read the stack trace instead.

Fast loop
- Compile what you touched before handing it back: `sbt "scarlet-tests/Test/compile"` when the change is in tests, or that module's `compile` when it is in main. A type error found here is a minute. The same error found after a full suite is much longer.
- Iterate on one suite: `make test-one T=com.ewoodbury.scarlet.core.TestActionContracts` (substitute the suite you changed).
- Run `make test` once before a pull request, not after every edit. A warm run is a few minutes and covers well over a thousand tests.
- `make test-lint` and `make test` each boot sbt. When both are due, use one process: `sbt ";checkFileLength; scalafixAll --check; scalafmtCheck; test"`.
- `make lint` rewrites formatting. `make test-lint` only checks. Format in the worktree that holds the change.
- `checkFileLength` fails above 1200 lines. 1200 is allowed. Aim near 1000.
- A wart warning does not fail lint. A scalafix import diff does. Do not restyle unrelated warnings.
- CI runs `make test-lint`, `sbt test`, PerfCheck, then `make test-spill`. Run PerfCheck when a change can send a query back to the row path. Run `make test-spill` when a change touches spill. Raise `SPILL_SUITE_FLOOR` when a spill suite is added.

One heavy build at a time
- Plan for a 16 GB machine. The default sbt process may take a 3 GB heap and 1 GB of metaspace, and the tests run inside that process (`fork := false`). One full `make test` is enough to fill the machine. A second full suite swaps or gets killed, and a killed JVM looks like a flaky test.
- `ScarletConf` and `ScarletRuntime` are global inside one JVM. A second sbt process has its own. Worktrees have separate `target/` directories and separate sbt servers, so two processes do not corrupt each other.
- Run one full suite, one PerfCheck, or one multi-gigabyte bench at a time.
- A second sbt may compile or run `make test-one`, with one explicit heap around 1.5 GB. `sbt --mem` does nothing when `-Xmx` is already set, so the small run needs a single heap flag and no second `-Xmx`.
- `make test-spill` launches sbt at 1 GB and forks the tests at 128 MB. It can run beside a review. It should not run beside a full suite or a large bench.
- Two 4 GB bench processes are the ceiling. Leave the suite idle while they run.
- The bloop daemon is not an sbt client. Look for `sbt-launch` before starting another build.

Branches
- Cut a branch from current `origin/main`. One branch owns a file while it is open. The primary checkout stays on `main`.
- `git worktree add -b` tracks `origin/main` until that upstream is unset. Unset it, then publish with `git push -u origin HEAD`.
- Commit in the worktree. Push and open the pull request after the gates for that change have passed.
- One imperative sentence per commit. Do not amend a commit that has been pushed.

Testing
- Tests run sequentially (`Test / parallelExecution := false`). Do not enable parallelism inside one JVM.
- `ScarletConf` is global mutable state. Restore it in `afterEach` or `finally`. `ScarletRuntime` uses the same set/restore pattern.
- Retry-related tests must set `baseRetryDelayMs = 1L`, or they take seconds.
- Cover behavior through `DistCollection` when possible. Unit-test internals only for invariants such as graph structure, partitioning, and sorting.
- A checksum match is not proof of the columnar path. `ColumnarFallback` extends `ClassCastException`, so a handler that treats it as a row rerun publishes no column batch and still matches the checksum. `ClosedKernelRefusal` does not extend `ClassCastException` and fails the action. A bench that exists to catch a silent row rerun must not catch `ColumnarFallback`.
- `ColumnMetrics` is process-global. Install a counting sink around the measured action and restore the previous sink in `finally`.
- A null fixture has to classify. A leading null component is not a pair witness, so the encode never runs. Put the null off row 0, and on a key the filter under test keeps. A null that the filter drops does not exercise the null path.
- The `spillEnabled` default stays false. A bench that turns spill on does it inside `main` and restores the saved configuration in `finally`. `groupByKey` does not spill. A change that makes it spill updates this sentence.
- Record a benchmark number only from a run you executed. Do not fill a table with a timing you did not print.

Code style
- Write functional code. All functions should be relatively short, testable, and pure.
- Write code that is readable to anyone with some experience with functional Scala.
- All Scala code should have strong type safety and compile-time checks.
- Use case classes when handling data types with fields. Use traits if there are common fields that should be shared across classes.
- Try to avoid for loops and while loops. Use functional patterns instead. A tight scan in the columnar kernel may keep a while. Do not rewrite that scan into an iterator chain.
- Use single-letter variables only for functions which are taken in as arguments. Use descriptive names for all named functions and for all variables.
- Do not use full classpaths in core code. Import relevant packages at the top of the file instead.
- Use a single format: one blank line after `object X {` and after `object X:` openers, matching scalafmt so `make lint` produces no diff.
- Keep each Scala file focused. Aim for about 1000 lines. `checkFileLength` fails the build above 1200.

WartRemover rules
- Avoid `var`, use `val` instead.
- When using .toLowerCase or .toUpperCase, pass in the English locale: `s.toUpperCase(java.util.Locale.ENGLISH)`.
- Avoid using `null`.
- Avoid writing code that results in inferred Any types. Type Anys explicitly.
- `asInstanceOf` and `Any` casts are allowed only at named erasure boundaries. Do not add a cast without a suppression annotation and a comment that says why it is safe.

Documentation
- Do not use all-caps or emojis in technical documentation or to-do lists.
- Keep documentation concise. Assume a senior engineer who is not yet deep in this engine.
- When a repeatable change makes the next edit faster or safer, update this file. Put the new rule here once it is true, in the same pull request when the change is small. Delete a rule when the code, the build, or the machine makes it false. Do not record the current task list, a timing from one run, or the name of a session. Keep each fact in one place.

Reviews
- Review the diff. Open another file only to confirm a finding.
- Do not edit a tree while a review is reading it.
- OpenCode keeps one local database, so run one OpenCode session at a time. A build and a review can run together.
