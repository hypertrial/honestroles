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

5. Smoke-test operability commands on a sample parquet:

```bash
$ honestroles init --input-parquet examples/jobs_sample.parquet --pipeline-config /tmp/pipeline.toml --plugins-manifest /tmp/plugins.toml --force
$ honestroles doctor --pipeline-config /tmp/pipeline.toml --format table
$ honestroles reliability check --pipeline-config /tmp/pipeline.toml --strict --format table
```

6. Commit and push `main`.

## Dedicated Ingestion Smoke Flow

Live ATS connector smoke tests run in a separate manual workflow:

- GitHub Actions workflow: `Ingestion Smoke` (`.github/workflows/ingest-smoke.yml`)
- Trigger: `workflow_dispatch`
- Inputs: `greenhouse_ref`, `lever_ref`, `ashby_ref`, `workable_ref`

Local equivalent command:

```bash
$ export HONESTROLES_SMOKE_GREENHOUSE_REF=stripe
$ export HONESTROLES_SMOKE_LEVER_REF=lever
$ export HONESTROLES_SMOKE_ASHBY_REF=notion
$ export HONESTROLES_SMOKE_WORKABLE_REF=microsoft
$ PYTHON_BIN=.venv/bin/python bash scripts/run_ingest_smoke.sh
```

This flow runs only `smoke`-marked live integration tests and is intentionally isolated from deterministic CI and coverage gates.

## Publish (Manual, API Token)

Publishing is manual and token-based.

Preferred local command:

```bash
$ bash scripts/publish_pypi.sh
```

`scripts/publish_pypi.sh` behavior:

- loads `PYPI_API_KEY` from current env or `.env`
- falls back to `PYPI_API_TOKEN` if present
- builds sdist/wheel
- runs `twine check`
- uploads via token auth (`TWINE_USERNAME=__token__`)

## Optional GitHub Manual Workflow

The `Release` GitHub Action is `workflow_dispatch` only (manual) and validates the requested version before upload.

If used, configure one repository secret:

- `PYPI_API_KEY` (preferred)
- `PYPI_API_TOKEN` (fallback)

Important: local `.env` values are not visible to GitHub runners.

## Post-Release Verification

After publish succeeds:

1. Install the released version in a clean environment.
2. Run `honestroles --help` and confirm commands are present (`init`, `doctor`, `reliability`, `ingest`, `runs`).
3. Run one lineage-writing command (`run`, `report-quality`, `adapter infer`, `eda`, `reliability check`, or `ingest sync`) and verify `.honestroles/runs/<run_id>/run.json` is created.
4. If you ran `reliability check`, confirm `dist/reliability/latest/gate_result.json` is written (or your custom `--output-file`).

## Common Publish Failures

### Missing Local API Key

Symptom:

```text
Missing PyPI API key. Set PYPI_API_KEY (or PYPI_API_TOKEN) in env or .env.
```

Fix:

1. Add `PYPI_API_KEY=<pypi-token>` to `.env` or shell env.
2. Re-run:

```bash
$ bash scripts/publish_pypi.sh
```

### Missing GitHub Secret (manual workflow path)

Symptom:

```text
Missing PyPI token secret. Set PYPI_API_KEY (or PYPI_API_TOKEN) in repository secrets.
```

Fix:

- Add one secret in `Settings -> Secrets and variables -> Actions`.
