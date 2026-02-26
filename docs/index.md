# HonestRoles

HonestRoles is a deterministic, config-driven pipeline runtime for job data.

## Architecture

- Polars-only in-memory model
- Explicit `HonestRolesRuntime`
- Instance-scoped `PluginRegistry` loaded from TOML manifest
- Fail-fast plugin and stage errors with typed exceptions

## CLI

```bash
honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
honestroles plugins validate --manifest plugins.toml
honestroles config validate --pipeline pipeline.toml
honestroles report-quality --pipeline-config pipeline.toml
honestroles scaffold-plugin --name my-plugin --output-dir .
```

See [Quickstart](start/quickstart.md) and [Plugin Manifest](reference/plugins.md).
