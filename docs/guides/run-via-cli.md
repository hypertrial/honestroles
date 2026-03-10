# Run via CLI

Run pipelines, validate configs/manifests, and inspect runs from the `honestroles` CLI.

## When to use

Use this for local runs, CI jobs, and operational scripts.

## Prerequisites

- HonestRoles installed
- A sample parquet input

## Steps

1. Optional: ingest from a public ATS source to create parquet input:

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe --format table
$ honestroles ingest validate --source greenhouse --source-ref stripe --quality-policy ingest_quality.toml --strict-quality --format table
```

Optional batch ingestion:

```bash
$ honestroles ingest sync-all --manifest ingest.toml --format table
```

2. Scaffold starter config files from a sample parquet:

```bash
$ honestroles init --input-parquet data/jobs.parquet --pipeline-config pipeline.toml --plugins-manifest plugins.toml
```

3. Validate environment and schema readiness:

```bash
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --format table
```

`doctor` exits with `0` for `pass|warn`, `1` for `fail`, and `2` for invalid inputs/config.

4. Run policy-aware reliability checks (writes a gate artifact):

```bash
$ honestroles reliability check --pipeline-config pipeline.toml --plugins plugins.toml --strict --output-file dist/reliability/latest/gate_result.json --format table
```

`--strict` keeps check severities unchanged but upgrades overall warn status to exit `1`.

5. Run pipeline execution:

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```

For human-readable CI logs, use table mode:

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml --format table
```

6. Validate manifest/config artifacts as needed:

```bash
$ honestroles plugins validate --manifest plugins.toml
$ honestroles config validate --pipeline pipeline.toml
$ honestroles report-quality --pipeline-config pipeline.toml --plugins plugins.toml
```

7. Inspect lineage records under `.honestroles/runs/`:

```bash
$ honestroles runs list --limit 20 --format table
$ honestroles runs list --command reliability.check --since 2026-01-01T00:00:00Z --contains-code POLICY_NULL_RATE --format table
$ honestroles runs list --command ingest.sync-all --since 2026-01-01T00:00:00Z --format table
$ honestroles runs list --command ingest.validate --since 2026-01-01T00:00:00Z --contains-code INGEST_QUALITY_NULL_RATE_COMPANY --format table
$ honestroles runs show --run-id <run_id>
```

8. Run EDA generation/diff/gate workflows:

```bash
$ honestroles eda generate --input-parquet jobs.parquet --output-dir dist/eda/latest
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate --output-dir dist/eda/diff
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
```

9. Launch the optional dashboard viewer:

```bash
$ honestroles eda dashboard --artifacts-dir dist/eda/latest --host 127.0.0.1 --port 8501
```

Dashboard note: table rendering uses Polars directly; `pyarrow` is not required.

## Expected result

Commands emit JSON payloads by default (or concise tables with `--format table`), return deterministic exit codes, and tracked executions are visible via `honestroles runs list`.

## Next steps

- Full command and exit-code table: [CLI Reference](../reference/cli.md)
- Manifest schema for batch ingestion: [Ingest Manifest Schema](../reference/ingest-manifest-schema.md)
- Ingestion quality policy schema: [Ingest Quality Policy Schema](../reference/ingest-quality-policy-schema.md)
- Connector/source-ref map: [Ingest Source-Ref Glossary](../reference/ingest-source-ref-glossary.md)
- Failure handling examples: [Common Errors](../troubleshooting/common-errors.md)
