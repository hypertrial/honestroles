# Historical Data Workflow

This guide documents the opt-in path for historical snapshot datasets such as
`jobs_historical.parquet`.

## Why historical mode exists

Historical datasets usually include:
- repeated snapshots of unchanged jobs
- listing/landing pages that are not concrete roles
- sparse enrichment fields (`salary_*`, arrays, visa flags)

`clean_historical_jobs(...)` is designed to address these traits without
changing default `clean_jobs(...)` behavior.

## Recommended flow

```python
import honestroles as hr

df = hr.read_parquet("jobs_historical.parquet", validate=False)
df = hr.clean_historical_jobs(df)
df = hr.filter_jobs(df, remote_only=False)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)
```

## Historical cleaning controls

`HistoricalCleanOptions`:
- `detect_listing_pages`
- `drop_listing_pages`
- `compact_snapshots`
- `prefer_existing_description_text`
- `snapshot_timestamp_output` (`"datetime"` by default, `"iso8601"` optional)
- `compaction_keys`
- `ingested_at_column`

By default, historical compaction keeps `first_seen` and `last_seen` as UTC
datetime values for faster processing. Use
`snapshot_timestamp_output="iso8601"` when downstream consumers require string
timestamps.

## Data quality reporting

One-shot report:

```python
report = hr.build_data_quality_report(df, dataset_name="jobs_historical")
print(report.to_dict())
```

Streaming report for large parquet files:

```bash
python scripts/report_data_quality.py jobs_historical.parquet --stream --format json
```

## Smoke testing

Historical smoke tests are opt-in and marked with `historical_smoke`:

```bash
pytest -o addopts="" -m "historical_smoke"
```
