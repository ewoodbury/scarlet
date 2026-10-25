#!/usr/bin/env bash
# Fails if tracked files look like review scratch, build droppings, or editor junk.
# Run: bash scripts/check-hygiene.sh   (also wired into CI and the pre-commit hook)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

pattern='(^|/)\.audit/|(^|/)\.DS_Store$|\.(diff|patch|log|err|orig|rej|bak|swp|tmp)$|(^|/)\.idea/|(^|/)target/|(^|/)\.bloop/|(^|/)\.metals/|(^|/)node_modules/'

bad="$(git ls-files | grep -E "$pattern" || true)"

if [ -n "$bad" ]; then
  echo "Repository hygiene check failed. These tracked files are artifacts, not source:" >&2
  echo "$bad" | sed 's/^/  /' >&2
  echo "" >&2
  echo "Remove them with: git rm --cached <path> and add the pattern to .gitignore." >&2
  exit 1
fi

echo "Repository hygiene check passed."
