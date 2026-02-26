# Choose Your Entry Point

## When to use this

Use this page to choose between the Python API, CLI workflows, and plugin extension model.

<div class="hr-callout">
  <strong>At a glance:</strong> API for composable pipelines, CLI for fast operational commands, plugins for custom organization logic.
</div>

## Prerequisites

- Installed `honestroles` package
- Clear goal: full data pipeline, quick command operation, or extension behavior

## Happy path

| Entry point | Choose this when | Primary surface |
|---|---|---|
| Library API | You are composing notebooks/ETL jobs and need stage-level control. | `import honestroles as hr` |
| CLI | You need repeatable shell operations and explicit exit behavior. | `honestroles-report-quality`, `honestroles-scaffold-plugin` |
| Plugin system | You need custom filter/label/rate behavior without forking core code. | plugin registration + entry points |

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

## Failure modes

- Using CLI when deep branch logic is needed can force brittle shell pipelines.
- Using API for one-off operational checks adds unnecessary overhead.
- Plugins should extend transformation behavior, not replace contract invariants.

Failure example:

```text
error: the following arguments are required: --name
```

## Related pages

- [Installation](installation.md)
- [Contract-First Quickstart](quickstart.md)
- [CLI Guide](../guides/cli.md)
- [Plugin Author Guide](../reference/plugins/author_guide.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>If you chose API, continue with <a href="quickstart.md">Contract-First Quickstart</a>.</li>
    <li>If you chose CLI, continue with <a href="../guides/cli.md">CLI Guide</a>.</li>
  </ul>
</div>
