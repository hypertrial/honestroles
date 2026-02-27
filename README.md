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
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
$ honestroles plugins validate --manifest plugins.toml
$ honestroles config validate --pipeline pipeline.toml
$ honestroles report-quality --pipeline-config pipeline.toml
$ honestroles scaffold-plugin --name my-plugin --output-dir .
```

## Python API

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",
)
result = runtime.run()

print(result.diagnostics)
print(result.dataframe.head())
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
```

For local profiling data, keep large parquet inputs under `data/` and write generated artifacts under `dist/` (both are ignored by git).

## License

MIT
