#!/usr/bin/env bash
set -euo pipefail

if ! command -v pre-commit >/dev/null 2>&1; then
  echo "pre-commit is required but not installed"
  exit 1
fi

pre-commit run trailing-whitespace --all-files
pre-commit run end-of-file-fixer --all-files
pre-commit run docs-refs --all-files
pre-commit run markdown-style --all-files
pre-commit run markdown-links --all-files
