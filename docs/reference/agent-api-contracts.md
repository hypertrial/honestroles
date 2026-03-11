# Agent API Contracts

Versioned JSON schema files for agent-facing request/response payloads:

- [contracts/agent_request.v1.json](/contracts/agent_request.v1.json)
- [contracts/agent_response.v1.json](/contracts/agent_response.v1.json)

## Request v1

Core fields:

- `candidate.profile_id`
- `candidate.skills`
- `candidate.titles`
- `candidate.locations`
- `candidate.work_mode_preferences`
- `candidate.seniority_targets`
- `candidate.salary_targets`
- `candidate.visa_work_auth`
- `top_k`
- optional `include_excluded`
- optional `policy_override`

## Response v1

Core fields:

- `schema_version`
- `status`
- `results[]` with:
  - `job_id`, `score`, `match_reasons`, `required_missing_skills`
  - `apply_url`, `posted_at`, `source`, `quality_flags`
- optional `excluded_jobs[]` with `exclude_reasons`
- optional `check_codes[]`

## Compatibility Policy

- Contracts are additive and versioned.
- New mandatory fields require a schema version bump.
- Next.js API routes should validate request/response payloads against these files.
