# HonestRoles

HonestRoles is a deterministic, config-driven pipeline runtime for job data with Polars and explicit plugin manifests.

## Start With the App

Use the HonestRoles app first: [honestroles.com](https://honestroles.com).

- Launch app: [https://honestroles.com](https://honestroles.com)
- App guide: [App Quickstart](https://honestroles.com/docs/app/get-started/)

## Choose Your Path

- App users: start in the browser at [honestroles.com](https://honestroles.com)
- Developers and integrators: use the CLI/SDK sections below

## Install (Developer)

```bash
$ python -m venv .venv
$ . .venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install honestroles
```

## 5-Minute First Run (Developer)

From the repository root:

```bash
$ python examples/create_sample_dataset.py
$ honestroles run --pipeline-config examples/sample_pipeline.toml --plugins examples/sample_plugins.toml
$ ls -lh examples/jobs_scored.parquet
```

Expected CLI diagnostics include `stage_rows`, `plugin_counts`, and `final_rows`.

## CLI

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe --quality-policy ingest_quality.toml --strict-quality --merge-policy updated_hash --retain-snapshots 30 --prune-inactive-days 90 --format table
$ honestroles ingest validate --source greenhouse --source-ref stripe --quality-policy ingest_quality.toml --strict-quality --format table
$ honestroles ingest sync-all --manifest ingest.toml --format table
$ honestroles init --input-parquet data/jobs.parquet --pipeline-config pipeline.toml --plugins-manifest plugins.toml
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --format table
$ honestroles reliability check --pipeline-config pipeline.toml --plugins plugins.toml --strict --format table
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
$ honestroles plugins validate --manifest plugins.toml
$ honestroles config validate --pipeline pipeline.toml
$ honestroles report-quality --pipeline-config pipeline.toml
$ honestroles runs list --limit 10 --command ingest.sync --format table
$ honestroles scaffold-plugin --name my-plugin --output-dir .
```

## Python API

```python
from honestroles import (
    HonestRolesRuntime,
    sync_source,
    sync_sources_from_manifest,
    validate_ingestion_source,
)

ingest = sync_source(
    source="greenhouse",
    source_ref="stripe",
    quality_policy_file="ingest_quality.toml",
    strict_quality=False,
    merge_policy="updated_hash",
    retain_snapshots=30,
    prune_inactive_days=90,
)
print(ingest.rows_written, ingest.output_parquet)

validation = validate_ingestion_source(
    source="greenhouse",
    source_ref="stripe",
    quality_policy_file="ingest_quality.toml",
    strict_quality=True,
)
print(validation.report.status, validation.rows_evaluated)

batch = sync_sources_from_manifest(manifest_path="ingest.toml")
print(batch.status, batch.total_sources, batch.fail_count)

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
result = runtime.run()

print(result.diagnostics)
print(result.dataset.to_polars().head())
print(result.application_plan[:3])
```

## Documentation

- App home: https://honestroles.com
- Docs home: https://honestroles.com/docs/
- Local docs source: `docs/`
- Start here in docs: `docs/index.md`

## Development

```bash
$ pip install -e ".[dev,docs]"
$ pytest -q
$ pytest tests/docs -q
$ bash scripts/check_docs_refs.sh
# Optional live connector smoke (requires refs):
# HONESTROLES_SMOKE_GREENHOUSE_REF, HONESTROLES_SMOKE_LEVER_REF,
# HONESTROLES_SMOKE_ASHBY_REF, HONESTROLES_SMOKE_WORKABLE_REF
$ bash scripts/run_ingest_smoke.sh
```

For local profiling data, keep large parquet inputs under `data/` and write generated artifacts under `dist/` (both are ignored by git).

## Maintainer Notes

- PyPI publishing is manual and token-based via `bash scripts/publish_pypi.sh`.
- The script reads `PYPI_API_KEY` (or `PYPI_API_TOKEN`) from env/`.env`.
- The GitHub `Release` workflow is manual (`workflow_dispatch`) only.
- Before publish, run deterministic gate:

```bash
$ PYTHON_BIN=.venv/bin/python bash scripts/run_coverage.sh
```

- Full maintainer runbook: `docs/for-maintainers/release-and-pypi.md`.

## License

MIT
