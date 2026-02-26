# Troubleshooting

## When to use this

Use this page when `honestroles` commands or pipeline stages fail and you need fast diagnosis with deterministic fixes.

<div class="hr-callout">
  <strong>At a glance:</strong> validate install first, reproduce with minimal command, map symptom to targeted fix.
</div>

## Prerequisites

- Access to failing command or script
- Local environment details (input path, file type, model/runtime settings)

## Happy path

Minimal debug sequence:

```bash
# 1) Verify command surfaces
honestroles-scaffold-plugin --help
honestroles-report-quality --help

# 2) Reproduce with smallest deterministic input
honestroles-report-quality jobs_current.parquet --format text

# 3) Run docs/build checks if changes were made
bash scripts/check_docs_refs.sh
mkdocs build --strict
```

Expected output (success):

```text
usage: honestroles-scaffold-plugin ...
usage: honestroles-report-quality ...
```

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `ValueError` in source contract validation | Missing required columns or invalid formats | Run normalize first, then validate; verify contract-required fields. |
| `Error: Input file not found: ...` | Wrong path or wrong working directory | Use absolute path or verify file presence before running command. |
| DuckDB error requiring `--table` | `.duckdb` input without `--table` or `--query` | Add `--table <name>` or `--query "select ..."`. |
| LLM fields missing | Ollama unavailable, model missing, or `use_llm=False` | Start Ollama, pull model, rerun with LLM flags. |
| `Backend 'hatchling.build' is not available` | Backend tooling absent in active env for no-isolation build | Install `.[dev]` or install `build` + `hatchling`. |
| Plugins not discovered | Wrong entrypoint group or plugin not installed | Use supported groups and reinstall plugin package in active env. |

Failure example:

```text
Error: --table is required for duckdb input when --query is not provided
```

## Related pages

- [CLI Guide](cli.md)
- [LLM Operations](llm_operations.md)
- [Plugin Author Guide](../reference/plugins/author_guide.md)
- [Source Data Contract](../reference/source_data_contract_v1.md)
- [FAQ](../reference/faq.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>After fixing issues, run the full path in <a href="end_to_end_pipeline.md">End-to-End Pipeline</a>.</li>
    <li>For persistent contract issues, revisit <a href="../start/quickstart.md">Quickstart</a>.</li>
  </ul>
</div>
