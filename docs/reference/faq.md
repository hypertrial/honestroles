# FAQ

## When to use this

Use this page for quick answers to common `honestroles` usage questions before diving into deeper reference docs.

<div class="hr-callout">
  <strong>At a glance:</strong> validate after normalize, prefer installed CLIs for normal usage, and treat plugin loading as environment-bound.
</div>

## Prerequisites

- Basic familiarity with the contract-first flow
- Awareness of whether you are using CLI commands or Python APIs

## Happy path

### When should I validate source data?

Read with `validate=False`, then run normalize and validate explicitly:

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)
```

### When should I use historical cleaning?

Use `clean_historical_jobs` for historical snapshots that require snapshot-aware compaction behavior.

### Do I need an extra package for LLM features?

No additional package extra is required for current local Ollama integration. You still need a running Ollama service.

### How are plugins loaded?

Plugins can be registered in code or discovered via entry points:

- `honestroles.filter_plugins`
- `honestroles.label_plugins`
- `honestroles.rate_plugins`

### Should I use console commands or script shims?

Use installed commands for normal usage. Use `scripts/*.py` shims as contributor compatibility helpers in repo workflows.

## Failure modes

- Validation called before normalize can produce avoidable format errors.
- Plugin discovery fails if plugin package is not installed in the active environment.
- Script shims can mask installation issues if used in place of public commands.

Failure example:

```text
Error: Input file not found: ...
```

## Related pages

- [Contract-First Quickstart](../start/quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Plugin Overview](plugins.md)
- [Source Data Contract](source_data_contract_v1.md)
- [Troubleshooting](../guides/troubleshooting.md)
