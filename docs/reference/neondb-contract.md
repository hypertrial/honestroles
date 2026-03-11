# NeonDB Contract

Reference contract for `honestroles publish neondb` under schema `honestroles_api`.

## Managed Tables

- `jobs_live`
- `job_features`
- `job_facets`
- `publish_batches`
- `feedback_events`
- `profile_weights`
- `profile_cache`
- `migration_history`

## Match Function

`honestroles_api.match_jobs_v1(candidate jsonb, top_k int, include_excluded boolean, policy_override jsonb)`

Returns rows with:

- `job_id`
- `score`
- `match_reasons`
- `required_missing_skills`
- `apply_url`
- `posted_at`
- `source`
- `quality_flags`
- `excluded`
- `exclude_reasons`

Determinism:

- Hard filters are applied before ranking.
- Weighted scoring uses default recommendation signals unless overridden.
- Tie-break order is `score DESC`, `posted_at DESC`, `job_id ASC`.

## Batch Metadata

`publish_batches` stores:

- `batch_id`
- `status`
- `started_at`, `finished_at`
- input/policy hashes
- inserted/updated/deactivated counts
- `quality_gate_status`

## Migration Ownership

The library owns all above objects and applies versioned migrations via:

```bash
$ honestroles publish neondb migrate --database-url-env NEON_DATABASE_URL --schema honestroles_api
```
