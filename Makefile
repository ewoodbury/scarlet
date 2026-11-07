.DEFAULT_GOAL := help

.PHONY: lint
lint: ## Run linting and formatting
	sbt ";checkFileLength; scalafixAll; scalafmt"

.PHONY: test-lint
test-lint: ## Run linting without fixing
	sbt ";checkFileLength; scalafixAll --check; scalafmtCheck"

.PHONY: test
test: ## Run tests
	sbt test

.PHONY: test-one
test-one: ## Run a single suite: make test-one T=com.ewoodbury.scarlet.core.TestLocalActions
	sbt "scarlet-tests/testOnly $(T)"

# The spill suites and the spill bench run in one forked JVM capped at 128 MB. The
# suites force spill with byte budgets; the bench asserts the file and merge counters.
# The cap is below what a whole-input materialisation needs: SpillBench OOMs at 64 MB
# and passes at 128 MB, so a path that retains its input instead of spilling fails here.
SPILL_SUITES := com.ewoodbury.scarlet.columnar.Test*Spill \
	com.ewoodbury.scarlet.columnar.TestSpillSum \
	com.ewoodbury.scarlet.columnar.TestSortMergeRuns \
	com.ewoodbury.scarlet.execution.TestRankSplitRuns \
	com.ewoodbury.scarlet.execution.TestRankSplitLongRuns \
	com.ewoodbury.scarlet.execution.TestSelectionSort

.PHONY: test-spill
test-spill: ## Run the spill suites and bench in a forked JVM with a 128 MB heap
	sbt -J-Xmx1g "set LocalProject(\"scarlet-tests\") / Test / fork := true" "set LocalProject(\"scarlet-tests\") / Test / javaOptions ++= Seq(\"-Xmx128m\", \"-XX:+ExitOnOutOfMemoryError\")" "scarlet-tests/testOnly $(SPILL_SUITES)" "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.SpillBench"

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
