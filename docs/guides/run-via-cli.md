# Run via CLI

Run pipelines, validate configs/manifests, and emit quality reports from the `honestroles` CLI.

## When to use

Use this for local runs, CI jobs, and operational scripts.

## Prerequisites

- HonestRoles installed
- A pipeline config and optional plugin manifest

## Steps

Run a pipeline:

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```

Validate plugin manifest:

```bash
$ honestroles plugins validate --manifest plugins.toml
```

Validate pipeline config:

```bash
$ honestroles config validate --pipeline pipeline.toml
```

Generate data quality report:

```bash
$ honestroles report-quality --pipeline-config pipeline.toml --plugins plugins.toml
```

Scaffold a plugin package:

```bash
$ honestroles scaffold-plugin --name my-plugin --output-dir .
```

## Expected result

Commands print JSON payloads or diagnostics and return deterministic exit codes.

## Next steps

- Full command and exit-code table: [CLI Reference](../reference/cli.md)
- Failure handling examples: [Common Errors](../troubleshooting/common-errors.md)
