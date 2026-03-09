# Release and PyPI

Maintainer runbook for versioned releases and PyPI publishing.

## Release Checklist

1. Bump `src/honestroles/__about__.py` to the new version.
2. Add release notes in `CHANGELOG.md`.
3. Run deterministic coverage gate:

```bash
$ PYTHON_BIN=.venv/bin/python bash scripts/run_coverage.sh
```

4. Run docs tests:

```bash
$ PYTHONPATH=src:plugin_template/src .venv/bin/python -m pytest tests/docs -q
```

5. Smoke-test new CLI operability commands on a sample parquet:

```bash
$ honestroles init --input-parquet examples/jobs_sample.parquet --pipeline-config /tmp/pipeline.toml --plugins-manifest /tmp/plugins.toml --force
$ honestroles doctor --pipeline-config /tmp/pipeline.toml --format table
```

6. Commit and push `main`.
7. Create/push tag `v<major>.<minor>.<patch>`.

The release workflow validates that tag version and package version match exactly.

## PyPI Authentication Model

Release workflow uses token-based upload with `pypa/gh-action-pypi-publish`.

Required repository secrets (either one):

- `PYPI_API_KEY`
- `PYPI_API_TOKEN`

Important: local `.env` values are not available in GitHub Actions runners.
If the workflow shows empty secret values, configure repository secrets in GitHub.

## Post-Release Verification

After publish succeeds:

1. Install the released version in a clean environment.
2. Run `honestroles --help` and confirm new commands are present (`init`, `doctor`, `runs`).
3. Run one command that writes lineage (`run`, `report-quality`, `adapter infer`, or `eda` command) and verify `.honestroles/runs/<run_id>/run.json` is created.

## Common Publish Failures

### Missing PyPI Secret

Symptom:

```text
Missing PyPI token secret. Set PYPI_API_KEY (or PYPI_API_TOKEN) in repository secrets.
```

Fix:

- Add a PyPI API token as `PYPI_API_KEY` (or `PYPI_API_TOKEN`) in:
  `Settings -> Secrets and variables -> Actions`.

### Trusted Publishing `invalid-publisher`

Symptom:

```text
invalid-publisher
```

Cause:

- PyPI trusted publisher claims do not match repository/workflow/tag claims.

Fix:

- Either configure trusted publisher claims correctly on PyPI, or use token secrets as above.

## Re-run Safety

Workflow uses `skip-existing: true`, so re-running publish for an already-uploaded version is safe.

## Emergency Manual Publish

If GitHub secrets are unavailable and local publish is required:

```bash
$ set -a
$ source .env
$ set +a
$ python -m build
$ TWINE_USERNAME=__token__ TWINE_PASSWORD="$PYPI_API_KEY" python -m twine upload dist/*
```

Use this only for maintainer-controlled recovery and keep CI release flow as the default path.
