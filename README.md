# HonestRoles

HonestRoles is a deterministic, config-driven pipeline runtime for job data with Polars and explicit plugin manifests.

## Install

```bash
$ python -m venv .venv
$ . .venv/bin/activate
$ python -m pip install --upgrade pip
$ pip install honestroles
```

## 5-Minute First Run

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

- Site: https://hypertrial.github.io/honestroles/
- Local docs source: `docs/`
- Start here in docs: `docs/index.md`

## Development

```bash
$ pip install -e ".[dev,docs]"
$ pytest -q
$ pytest tests/docs -q
$ bash scripts/check_docs_refs.sh
```

## License

MIT
