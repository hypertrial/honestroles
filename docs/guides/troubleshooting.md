# Troubleshooting

## Purpose

This page lists common `honestroles` failures and concrete fixes for users and maintainers.

## Public API / Interface

Troubleshooting targets:

- source-data validation (`normalize_source_data_contract`, `validate_source_data_contract`)
- CLI command execution
- DuckDB/parquet input handling
- local LLM/Ollama features
- packaging/build checks
- plugin loading through entry points

## Usage Example

Typical debug flow:

```bash
# 1) Confirm installation and CLI availability
honestroles-scaffold-plugin --help
honestroles-report-quality --help

# 2) Run a deterministic data-quality command first
honestroles-report-quality jobs_current.parquet --format text

# 3) Build docs and run checks if you changed docs
bash scripts/check_docs_refs.sh
mkdocs build --strict
```

## Edge Cases and Errors

| Symptom | Likely Cause | Fix |
|---|---|---|
| `ValueError` from contract validation | Missing required fields or invalid formats | Run `normalize_source_data_contract` before validation and check required source fields in `reference/source_data_contract_v1.md`. |
| `Error: Input file not found: ...` | Wrong path or working directory | Use absolute paths or confirm the file exists before running CLI command. |
| DuckDB CLI error about missing table/query | `.duckdb` input without `--table` and without `--query` | Pass `--table <name>` or `--query "select ..."`. |
| LLM labels/ratings are missing or unchanged | Ollama server unavailable or `use_llm=False` | Start Ollama (`ollama serve`), verify model availability, set `use_llm=True`. |
| `ERROR Backend 'hatchling.build' is not available` during local build | Backend tooling not installed in active env for `--no-isolation` build | Install `.[dev]` or `pip install build hatchling` before `python -m build --no-isolation`. |
| Plugin entry points do not load | Wrong entry point group or package not installed | Use supported groups (`honestroles.filter_plugins`, `honestroles.label_plugins`, `honestroles.rate_plugins`) and reinstall plugin package. |

## Related Pages

- [Quickstart](../start/quickstart.md)
- [CLI Guide](cli.md)
- [LLM Operations](llm_operations.md)
- [Plugin Author Guide](../reference/plugins/author_guide.md)
