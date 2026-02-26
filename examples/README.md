# Examples

## First Run Assets

- `create_sample_dataset.py`: writes `examples/jobs_sample.parquet`
- `sample_pipeline.toml`: runtime config for sample dataset
- `sample_plugins.toml`: minimal plugin manifest

## Execute

```bash
$ python examples/create_sample_dataset.py
$ honestroles run --pipeline-config examples/sample_pipeline.toml --plugins examples/sample_plugins.toml
$ python examples/run_runtime.py
```
