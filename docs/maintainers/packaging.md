# Packaging

## Purpose

This page documents packaging checks required to keep `honestroles` distribution-ready.

## Public API / Interface

Primary packaging commands:

- `python -m build --sdist --wheel --no-isolation`
- `python -m twine check dist/*`

Required build artifacts and metadata:

- wheel includes template assets under `honestroles/_templates/plugin_template`
- wheel includes `honestroles/py.typed`
- wheel entry points expose:
  - `honestroles-scaffold-plugin`
  - `honestroles-report-quality`

Installed-wheel smoke coverage:

- see `tests/test_packaging_distribution.py`
- verifies wheel install behavior from non-repo working directory

## Usage Example

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"

python -m build --sdist --wheel --no-isolation
python -m twine check dist/*
pytest tests/test_packaging_distribution.py -q -o addopts=""
```

## Edge Cases and Errors

- `Backend 'hatchling.build' is not available` means backend tooling is missing in active env; reinstall with `pip install -e ".[dev]"`.
- If scaffold CLI fails after wheel install, verify template assets exist in the wheel archive.
- If entry points are missing, inspect `entry_points.txt` in `.dist-info` output.

## Related Pages

- [Release Process](release_process.md)
- [Docs Stack](docs_stack.md)
- CI Workflow: `.github/workflows/ci.yml`
- Release Workflow: `.github/workflows/release.yml`
