# Agent-First Implementation Roadmap

This roadmap tracks delivery for AI-agent-first job seeking workflows with strict Ollama usage.

## Phase Status

- [x] Phase 1: Reliability hardening (DuckDB + normalization regressions fixed)
- [x] Phase 2: Agent-first matching primitives (`CandidateProfile`, signal extraction, ranking)
- [x] Phase 3: Ollama-only signal extraction path (heuristic-first with Ollama fallback)
- [x] Phase 4: New-grad MDS scoring profile and explainable output columns
- [x] Phase 5: Microproduct-facing APIs (`extract_job_signals`, `rank_jobs`, `build_application_plan`)
- [x] Phase 6: Docs/examples refresh for agent builder workflows

## Active Public Workflow

1. `clean_jobs`
2. `label_jobs`
3. `rate_jobs`
4. `rank_jobs(profile=CandidateProfile.mds_new_grad())`
5. `build_application_plan(...)`

## Output Contract For Agents

- `fit_score`
- `fit_breakdown`
- `missing_requirements`
- `why_match`
- `next_actions`
- signal columns:
  - `required_skills_extracted`
  - `preferred_skills_extracted`
  - `experience_years_min`
  - `experience_years_max`
  - `entry_level_likely`
  - `visa_sponsorship_signal`
  - `application_friction_score`
  - `role_clarity_score`
  - `signal_confidence`
  - `signal_source`
  - `signal_reason`
