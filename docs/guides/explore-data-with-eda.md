# Explore Data with EDA

Generate deterministic EDA artifacts from a parquet input, then optionally inspect them in a local dashboard.

## When to use

Use this guide when you want repeatable profiling artifacts that can be checked into CI or shared in PRs.

## Prerequisites

- HonestRoles installed
- For dashboard mode: `pip install "honestroles[eda]"`

## Steps

Generate artifacts:

```bash
$ honestroles eda generate --input-parquet jobs_historical.parquet --output-dir dist/eda/latest
```

Review generated artifacts:

```bash
$ ls -R dist/eda/latest
$ cat dist/eda/latest/report.md
$ cat dist/eda/latest/summary.json
```

Launch the optional Streamlit view layer:

```bash
$ honestroles eda dashboard --artifacts-dir dist/eda/latest --host 127.0.0.1 --port 8501
```

## Expected result

`dist/eda/latest` contains:

- `manifest.json`
- `summary.json`
- `report.md`
- `tables/*.parquet`
- `figures/*.png`

The dashboard renders these files directly and does not run profiling logic.

## Next steps

- Compare artifacts between runs to verify data extraction improvements.
- Update quality profile weights and regenerate to align scoring with your use case.
