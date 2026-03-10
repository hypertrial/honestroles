#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
PYTEST_PATH="${PYTEST_PATH:-tests/test_ingest_smoke_live.py}"

required_envs=(
  HONESTROLES_SMOKE_GREENHOUSE_REF
  HONESTROLES_SMOKE_LEVER_REF
  HONESTROLES_SMOKE_ASHBY_REF
  HONESTROLES_SMOKE_WORKABLE_REF
)

missing=()
for key in "${required_envs[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    missing+=("$key")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required env vars for ingestion smoke:"
  for key in "${missing[@]}"; do
    echo " - $key"
  done
  exit 2
fi

export HONESTROLES_RUN_INGEST_SMOKE=1
PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" -m pytest "$PYTEST_PATH" -m "smoke" -q -o addopts=""
