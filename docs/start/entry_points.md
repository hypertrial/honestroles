# Entry Points

## Purpose

This page helps you choose the right interface for `honestroles`: library API, CLI, or plugins.

## Public API / Interface

Decision matrix:

| Entry Point | Choose This When | Primary Surface |
|---|---|---|
| Library API | You are building notebooks, ETL jobs, or full pipelines in Python. | `import honestroles as hr` |
| CLI | You need quick diagnostics or plugin scaffolding from shell. | `honestroles-report-quality`, `honestroles-scaffold-plugin` |
| Plugin System | You need custom filter/label/rate behavior without forking core code. | plugin registration and entry points |

Choose this when examples:

- Library API: "I want `clean -> filter -> label -> rate -> rank` in a notebook."
- CLI: "I want a quick quality report for a parquet file."
- Plugins: "I need to add a custom rating transform for my org."

## Usage Example

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.normalize_source_data_contract(df)
df = hr.validate_source_data_contract(df)
df = hr.clean_jobs(df)
```

```bash
honestroles-report-quality jobs_historical.parquet --stream --format text
honestroles-scaffold-plugin --name honestroles-plugin-myorg --output-dir .
```

## Edge Cases and Errors

- Use the library API when you need row-level debugging or custom branching between stages.
- Use the CLI when you need deterministic command behavior and direct exit codes.
- Use plugins only for extension logic; do not replace contract validation or schema invariants inside plugins.

## Related Pages

- [Quickstart](quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Plugin Author Guide](../reference/plugins/author_guide.md)
- [Framework](../concepts/framework.md)
