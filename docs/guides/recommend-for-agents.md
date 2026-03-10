# Recommend for Agents

## When to use
Use this flow when your Next.js API needs deterministic, explainable job recommendations from cleaned parquet jobs.

## Prerequisites
- Cleaned parquet jobs available (for example from `honestroles ingest sync`).
- Optional policies in repo root:
  - `recommendation.toml`
  - `recommend_eval.toml`
- Candidate input as either:
  - strict profile JSON
  - plain text resume

## Steps
### 1) Build index artifacts
```bash
$ honestroles recommend build-index \
  --input-parquet dist/ingest/greenhouse/stripe/jobs.parquet \
  --policy recommendation.toml
```

Default output is `dist/recommend/index/<index_id>/` with:

- `manifest.json`
- `jobs_latest.jsonl`
- `facets.json`
- `shards/*.json`
- `quality_summary.json`

### 2) Match candidate to jobs
Candidate JSON:
```bash
$ honestroles recommend match \
  --index-dir dist/recommend/index/<index_id> \
  --candidate-json examples/candidate.json \
  --top-k 25 \
  --include-excluded \
  --policy recommendation.toml
```

Resume plain text:
```bash
$ honestroles recommend match \
  --index-dir dist/recommend/index/<index_id> \
  --resume-text examples/resume.txt \
  --profile-id jane_doe \
  --top-k 25
```

Output includes agent-ready rows:

- `job_id`
- `score`
- `match_reasons`
- `required_missing_skills`
- `apply_url`
- `posted_at`
- `source`
- `quality_flags`

When `--include-excluded` is set, output also includes `excluded_jobs` with deterministic `exclude_reasons` codes.

### 3) Evaluate recommendation quality offline
```bash
$ honestroles recommend evaluate \
  --index-dir dist/recommend/index/<index_id> \
  --golden-set examples/recommend_golden_set.json \
  --thresholds recommend_eval.toml
```

The command computes `precision@k` and `recall@k` and exits non-zero when thresholds fail.

### 4) Record feedback for incremental personalization
```bash
$ honestroles recommend feedback add \
  --profile-id jane_doe \
  --job-id 12345 \
  --event interviewed

$ honestroles recommend feedback summarize --profile-id jane_doe
```

Feedback files are stored locally under `.honestroles/recommend/feedback/`.

## Expected result
- `build-index` writes deterministic retrieval artifacts for fast API reads.
- `match` returns agent-ready result rows with explainability and optional exclusions.
- `evaluate` enforces relevance thresholds for regression control.
- `feedback` updates per-profile weighting state for low-compute personalization.

## Next steps
- Serve `jobs_latest.jsonl`, facets, and shard files from your Next.js API route layer.
- Schedule `recommend build-index` after ingestion refreshes.
- Add `recommend evaluate` to CI with a maintained golden set.
