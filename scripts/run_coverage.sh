#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"

PYTHONPATH=src:plugin_template/src "$PYTHON_BIN" -m pytest tests plugin_template/tests \
  -m "not fuzz" -o addopts="" \
  --cov=src/honestroles \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-report=json:coverage.json \
  --cov-fail-under=100 -q
