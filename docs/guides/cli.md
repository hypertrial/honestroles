# CLI Guide

## Purpose

This page documents the two supported `honestroles` CLI commands, including options, examples, and failure behavior.

## Public API / Interface

### `honestroles-scaffold-plugin`

Create a new plugin package from the built-in template.

Options:

- `--help`: Show help and exit `0`.
- `--name` (required): Plugin distribution name.
- `--package`: Optional Python package name override.
- `--output-dir`: Output directory for generated package (default `.`).
- `--force`: Overwrite destination directory if it exists.

### `honestroles-report-quality`

Build a quality report for parquet or duckdb inputs.

Arguments and options:

- `input` (required positional): Path to `.parquet`, `.duckdb`, or `.db` file.
- `--help`: Show help and exit `0`.
- `--format`: `text` or `json` (default `text`).
- `--dataset-name`: Optional label override for report output.
- `--stream`: Stream parquet row groups instead of loading full file.
- `--table`: DuckDB table name (required for duckdb input unless `--query` is set).
- `--query`: Read-only DuckDB query to analyze.
- `--top-n-duplicates`: Number of duplicate values to include (default `10`).

Installed package and contributor script shims:

- Installed commands: `honestroles-scaffold-plugin`, `honestroles-report-quality`
- Contributor fallbacks: `python scripts/scaffold_plugin.py`, `python scripts/report_data_quality.py`

## Usage Example

Valid examples:

```bash
honestroles-scaffold-plugin \
  --name honestroles-plugin-acme \
  --output-dir ./plugins

honestroles-report-quality jobs_historical.parquet --stream --format json
honestroles-report-quality jobs.duckdb --table jobs_current --format text
honestroles-report-quality jobs.duckdb --query "select * from jobs_current" --top-n-duplicates 20
```

Invalid examples and expected failures:

```bash
# Missing file path -> non-zero exit with:
# Error: Input file not found: missing.parquet
honestroles-report-quality missing.parquet

# DuckDB input without --table or --query -> non-zero exit with:
# Error: --table is required for duckdb input when --query is not provided
honestroles-report-quality jobs.duckdb
```

## Edge Cases and Errors

- `--help` should always exit with code `0`.
- Parser/argument failures (for example, missing required `--name`) exit non-zero via argparse.
- Runtime failures print `Error: ...` to stderr and exit `1`.
- For duckdb inputs, prefer `--query` for filtered slices and `--table` for full-table checks.

## Related Pages

- [Installation](../start/installation.md)
- [Entry Points](../start/entry_points.md)
- [Troubleshooting](troubleshooting.md)
- [IO Reference](../reference/io.md)
