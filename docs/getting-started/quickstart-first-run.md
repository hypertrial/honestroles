# Quickstart (First Run)

This workflow guarantees a successful first run from a clean checkout.

Prefer the browser product? Start with [https://honestroles.com](https://honestroles.com).

## When to use

Use this for your first end-to-end pipeline execution.

## Prerequisites

- HonestRoles installed
- Repository root as working directory

## Steps

1. Create a tiny sample dataset:

```bash
$ python - <<'PY'
import polars as pl

pl.DataFrame(
    {
        "id": ["1", "2", "3"],
        "title": ["Data Engineer", "Senior ML Engineer", "Intern Analyst"],
        "company": ["A", "B", "C"],
        "location": ["Remote", "NYC", "Remote"],
        "remote": ["true", "false", "1"],
        "description_text": [
            "Python SQL data pipelines",
            "Build ML systems with Python and AWS",
            "Excel and reporting",
        ],
        "description_html": ["<p>Python SQL</p>", "<b>ML</b>", "<i>intern</i>"],
        "skills": ["python,sql", "python,aws", None],
        "salary_min": [120000, 180000, None],
        "salary_max": [160000, 220000, None],
        "apply_url": ["https://example.com/1", "https://example.com/2", "https://example.com/3"],
        "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03"],
    }
).write_parquet("examples/jobs_sample.parquet")
PY
```

2. Scaffold starter config files from the sample input:

```bash
$ honestroles init --input-parquet examples/jobs_sample.parquet --pipeline-config pipeline.toml --plugins-manifest plugins.toml
```

3. Validate readiness before running:

```bash
$ honestroles doctor --pipeline-config pipeline.toml --plugins plugins.toml --format table
```

4. Run policy gate checks (recommended for CI):

```bash
$ honestroles reliability check --pipeline-config pipeline.toml --plugins plugins.toml --strict --format table
```

This writes `dist/reliability/latest/gate_result.json` by default.

5. Run the pipeline:

```bash
$ honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```

6. Verify output file exists:

```bash
$ ls -lh dist/jobs_scored.parquet
```

7. Verify run lineage was recorded:

```bash
$ honestroles runs list --limit 5 --command reliability.check --format table
```

## Expected result

The CLI prints JSON diagnostics by default (or table output with `--format table`).

The output file `dist/jobs_scored.parquet` should exist.

!!! warning
    If this fails, go to [Common Errors](../troubleshooting/common-errors.md).

## Next steps

- Understand what each config section controls: [First Pipeline Config](first-pipeline-config.md)
- See command behavior and flags: [CLI Reference](../reference/cli.md)
