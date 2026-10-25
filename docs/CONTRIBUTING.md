# Contributing

## Repository hygiene

The repository contains source, tests, docs, build configuration, and project scripts. Nothing else.

- Scratch files — diffs, review prompts, review notes, plans, logs, benchmark transcripts — live **outside** the repository, in a sibling directory such as `../.scarlet-queue/`.
- Never stage with `git add -A` or `git add .`. Stage explicit paths.
- Before committing, run `git status --short` and `bash scripts/check-hygiene.sh`, and read the staged list. If a file is not source, test, doc, or build configuration, it does not belong in the commit.
- Enable the pre-commit hook once per clone:

  ```bash
  git config core.hooksPath .githooks
  ```

- CI runs the same check on every push and pull request.
- A pull request body lists its changed files, and reviewers flag any file that is not source, test, doc, or build configuration.

## Before opening a pull request

```bash
make lint
make test-lint
make test
make hygiene
```

All four must pass. `make hygiene` fails when a tracked file looks like review scratch or a build artifact.
