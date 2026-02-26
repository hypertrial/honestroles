# FAQ

## Purpose

This page answers common usage and maintainer questions for `honestroles`.

## Public API / Interface

### When should I validate source data?

Use `validate=False` at initial read boundaries, then run:

1. `normalize_source_data_contract`
2. `validate_source_data_contract`

before clean/filter/label/rate stages.

### When should I use historical cleaning?

Use `clean_historical_jobs` for historical snapshot datasets where compaction and historical-specific handling are expected.

### Do I need an extra package for LLM features?

No extra dependency is required for the current local Ollama integration. You still need a running Ollama service.

### How are plugins loaded?

Plugins can be registered directly in code or discovered via entry points (`honestroles.filter_plugins`, `honestroles.label_plugins`, `honestroles.rate_plugins`).

### Should I use console commands or script shims?

Use installed commands (`honestroles-scaffold-plugin`, `honestroles-report-quality`) for normal usage. Script shims in `scripts/` are compatibility helpers for repository contributors.

## Usage Example

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)
```

## Edge Cases and Errors

- Skipping normalization before validation often causes avoidable format failures.
- Entry-point plugin loading requires plugin package installation in the same environment.
- CLI script shims may work from source tree even when package installation is missing; treat them as contributor tooling only.

## Related Pages

- [Quickstart](../start/quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Plugin Overview](plugins.md)
- [Source Data Contract](source_data_contract_v1.md)
