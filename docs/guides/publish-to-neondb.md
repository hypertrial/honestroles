# Publish to NeonDB

## When to use
Use this flow when HonestRoles artifacts are produced locally and a separate Next.js app must read a stable database contract for agent-facing APIs.

## Prerequisites
- A Neon/Postgres connection string exported in `NEON_DATABASE_URL`.
- Cleaned jobs parquet and recommendation index artifacts already built:
  - `honestroles ingest sync` or `honestroles run`
  - `honestroles recommend build-index`
- Optional DB dependency installed for CLI usage:

```bash
$ pip install -e ".[db]"
```

## Steps
1. Apply schema migrations:

```bash
$ honestroles publish neondb migrate --database-url-env NEON_DATABASE_URL --schema honestroles_api --format table
```

2. Publish latest jobs + features into Neon:

```bash
$ honestroles publish neondb sync \
  --database-url-env NEON_DATABASE_URL \
  --schema honestroles_api \
  --jobs-parquet dist/ingest/greenhouse/stripe/jobs.parquet \
  --index-dir dist/recommend/index/<index_id> \
  --sync-report dist/ingest/greenhouse/stripe/sync_report.json \
  --require-quality-pass \
  --format table
```

3. Verify DB contract and function health:

```bash
$ honestroles publish neondb verify --database-url-env NEON_DATABASE_URL --schema honestroles_api --format table
```

4. Query from Next.js API using SQL function:

```sql
SELECT *
FROM honestroles_api.match_jobs_v1(
  '{"profile_id":"jane","skills":["python","sql"],"locations":["remote"],"work_mode_preferences":["remote"]}'::jsonb,
  25,
  false,
  '{}'::jsonb
);
```

## Expected result
- `migrate` reports latest migration applied in `honestroles_api.migration_history`.
- `sync` writes/upserts `jobs_live`, `job_features`, `job_facets`, and updates `publish_batches`.
- `verify` returns `pass` with all required tables/functions present.
- Next.js API can execute `match_jobs_v1(...)` without invoking Python at request time.

## Next steps
- Wire the Next.js API route to call `honestroles_api.match_jobs_v1(...)`.
- Schedule periodic `publish neondb sync` after ingestion/index refreshes.
- Add `publish neondb verify` to deployment checks before API rollouts.

## Manual CI Smoke
Use the manual GitHub Actions workflow `NeonDB Smoke` for end-to-end contract validation:

1. Configure repository secret `NEON_DATABASE_URL`.
2. Trigger workflow `NeonDB Smoke` (default schema `honestroles_api`).
3. The workflow runs:
   - `publish neondb migrate`
   - `publish neondb sync` (sample artifacts generated in workflow)
   - `publish neondb verify`
   - a live `match_jobs_v1(...)` query with contract-key assertions.
