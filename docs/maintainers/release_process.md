# Release Process

## Purpose

This page defines a repeatable release checklist for `honestroles`.

## Public API / Interface

Versioning and tags:

- package version source: `src/honestroles/__about__.py`
- release tag format: `v<major>.<minor>.<patch>`
- release workflow validates tag/version alignment

Pre-release checks:

- `pytest -q`
- `bash scripts/check_docs_refs.sh`
- `mkdocs build --strict`
- `python -m build --sdist --wheel --no-isolation`
- `python -m twine check dist/*`
- `pytest tests/test_packaging_distribution.py -q -o addopts=""`

Post-release verification:

- install package in clean environment
- confirm both console scripts run from non-repo directory
- verify docs remain accessible and consistent

## Usage Example

```bash
# 1) Update version in src/honestroles/__about__.py
# 2) Run release checks locally
pytest -q
bash scripts/check_docs_refs.sh
mkdocs build --strict
python -m build --sdist --wheel --no-isolation
python -m twine check dist/*

# 3) Commit + push, then tag
git tag v0.1.1
git push origin v0.1.1
```

## Edge Cases and Errors

- If release workflow fails tag validation, ensure tag matches package version exactly.
- If docs fail strict build, resolve link/reference warnings before tagging.
- If PyPI publish fails after build succeeds, confirm artifact integrity via `twine check` and rerun workflow.

## Related Pages

- [Packaging](packaging.md)
- [Compatibility and Versioning](../concepts/compatibility_and_versioning.md)
- Changelog: `CHANGELOG.md`
