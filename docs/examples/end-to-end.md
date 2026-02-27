# End-to-End Example

This example mirrors a full runtime-integrator flow using repository sample files.

## Files

- `examples/sample_pipeline.toml`
- `examples/sample_plugins.toml`
- `examples/example_plugins.py`

## Run

```bash
$ python examples/create_sample_dataset.py
$ honestroles run --pipeline-config examples/sample_pipeline.toml --plugins examples/sample_plugins.toml
```

## Inspect Output

```bash
$ python examples/run_runtime.py
$ honestroles eda generate --input-parquet examples/jobs_sample.parquet --output-dir dist/eda/latest
$ cat dist/eda/latest/report.md
```

## Validate Inputs

```bash
$ honestroles config validate --pipeline examples/sample_pipeline.toml
$ honestroles plugins validate --manifest examples/sample_plugins.toml
```

## Related

- Output semantics: [Understand Output](../guides/understand-output.md)
- CLI contract: [CLI Reference](../reference/cli.md)
