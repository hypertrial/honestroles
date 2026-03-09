#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [ -z "${PYPI_API_KEY:-}" ] && [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [ -z "${PYPI_API_KEY:-}" ] && [ -n "${PYPI_API_TOKEN:-}" ]; then
  export PYPI_API_KEY="${PYPI_API_TOKEN}"
fi

if [ -z "${PYPI_API_KEY:-}" ]; then
  echo "Missing PyPI API key. Set PYPI_API_KEY (or PYPI_API_TOKEN) in env or .env." >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
if [ -x ".venv/bin/python" ] && [ "${PYTHON_BIN}" = "python3" ]; then
  PYTHON_BIN=".venv/bin/python"
fi

"${PYTHON_BIN}" -m pip install --upgrade pip
"${PYTHON_BIN}" -m pip install build twine
rm -rf dist/*
"${PYTHON_BIN}" -m build
"${PYTHON_BIN}" -m twine check dist/*
TWINE_USERNAME=__token__ TWINE_PASSWORD="${PYPI_API_KEY}" "${PYTHON_BIN}" -m twine upload dist/*

echo "Published honestroles to PyPI using API token auth."
