# CLI Guide

## When to use this

Use this page when running public `honestroles` CLI commands for quality diagnostics or plugin scaffolding.

<div class="hr-callout">
  <strong>At a glance:</strong> one command for quality checks, one for plugin scaffolding, both with stable exit semantics.
</div>

## Prerequisites

- Installed package (`honestroles`) or contributor script shims in source tree
- Input parquet/duckdb files for quality reports

## Happy path

### `honestroles-scaffold-plugin`

- `--help`: Show help and exit `0`
- `--name` (required): Plugin distribution name
- `--package`: Optional Python package override
- `--output-dir`: Output directory (default `.`)
- `--force`: Overwrite destination if it exists

```bash
honestroles-scaffold-plugin \
  --name honestroles-plugin-acme \
  --output-dir ./plugins
```

Expected output (success):

```text
Scaffold created at: /.../plugins/honestroles-plugin-acme
```

### `honestroles-report-quality`

- `input` (required positional): path to `.parquet`, `.duckdb`, or `.db`
- `--help`: Show help and exit `0`
- `--format`: `text|json` (default `text`)
- `--dataset-name`: Optional report label
- `--stream`: Stream parquet row groups
- `--table`: DuckDB table name (required when `--query` is absent)
- `--query`: Read-only DuckDB query
- `--top-n-duplicates`: duplicate hotspot count (default `10`)

```bash
honestroles-report-quality jobs_historical.parquet --stream --format json
honestroles-report-quality jobs.duckdb --table jobs_current --format text
```

Choose this path if...

- Use CLI for reproducible one-shot operations and shell-friendly exit codes.
- Use Python API for custom branching, row-level debugging, or pipeline composition.

Contributor fallback shims:

```bash
python scripts/scaffold_plugin.py --name honestroles-plugin-myorg --output-dir .
python scripts/report_data_quality.py jobs_historical.parquet --stream --format json
```

## Failure modes

Invalid examples and deterministic failures:

```bash
# Missing input file
honestroles-report-quality missing.parquet
# Error: Input file not found: missing.parquet

# DuckDB input without --table or --query
honestroles-report-quality jobs.duckdb
# Error: --table is required for duckdb input when --query is not provided
```

- argparse-level input errors return non-zero immediately
- runtime errors print `Error: ...` and exit `1`

## Related pages

- [Installation](../start/installation.md)
- [Choose Your Entry Point](../start/entry_points.md)
- [Troubleshooting](troubleshooting.md)
- [IO Reference](../reference/io.md)
- [FAQ](../reference/faq.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>Need a full data flow? Continue to <a href="end_to_end_pipeline.md">End-to-End Pipeline</a>.</li>
    <li>Investigating failures? Use <a href="troubleshooting.md">Troubleshooting</a>.</li>
  </ul>
</div>
