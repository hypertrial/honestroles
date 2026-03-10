# Changelog

## Unreleased

- Added Ingestion Reliability Hotfix Bundle:
  - Fixed URL dedup keying to preserve identity query params (`gh_jid`, `job_id`, `jobid`, `posting_id`, `position_id`) while still dropping tracking params.
  - Added Greenhouse repeated-page detection (`INGEST_PAGE_REPEAT_DETECTED`) with coverage-incomplete semantics.
  - Fixed `honestroles init` adapter inference on list-typed columns by making string parse scoring list-safe.
  - Improved Workable invalid/non-public subdomain errors with actionable validation guidance.
  - Aligned `--write-raw` default path with explicit `--output-parquet` roots and updated lineage artifact inference.
  - Strengthened live ingestion smoke assertions to require non-empty outputs for configured sources.
  - Updated ingestion docs/troubleshooting/source-ref guidance for pagination warnings, Workable expectations, and raw artifact path behavior.
- Added Ingestion Reliability & Operability v3:
  - Added `honestroles ingest validate` for fetch+normalize+quality evaluation without latest overwrite.
  - Extended `honestroles ingest sync` with `--quality-policy`, `--strict-quality`, `--merge-policy`, `--retain-snapshots`, and `--prune-inactive-days`.
  - Added ingestion quality policy subsystem (`ingest_quality.toml`) with built-in defaults, deterministic check codes, and strict-quality escalation behavior.
  - Added pre-write quality gating and additive report fields (`quality_status`, `quality_summary`, `quality_check_codes`, `stage_timings_ms`, `warnings`).
  - Added deterministic merge policy controls (`updated_hash`, `first_seen`, `last_seen`) for catalog/latest conflict resolution.
  - Added snapshot retention and inactive catalog compaction controls with report counters.
  - Added ingestion lineage expansions: `ingest.validate` tracking and ingest metrics/timing payloads.
  - Added fixture-backed connector conformance tests under `tests/fixtures/ingest/*`.
  - Updated ingestion docs (CLI/reference/guides/troubleshooting/runtime API) for new controls and policy schema.
- Added Ingestion v2:
  - Added `honestroles ingest sync-all --manifest ingest.toml` for deterministic multi-source batch orchestration.
  - Extended `honestroles ingest sync` with HTTP runtime controls (`--timeout-seconds`, `--max-retries`, `--base-backoff-seconds`, `--user-agent`).
  - Added manifest loader/API surface (`sync_sources_from_manifest`) and `ingest.toml` defaults+per-source override model.
  - Added snapshot + latest storage model with per-run snapshot parquet and catalog-backed latest rebuild.
  - Upgraded incremental semantics to v2 state with posted+updated watermarks and coverage-aware tombstones.
  - Added additive normalization fields: `source_updated_at`, `work_mode`, `salary_currency`, `salary_interval`, `employment_type`, `seniority`.
  - Extended ingestion reports with lifecycle and telemetry fields (`new_count`, `updated_count`, `unchanged_count`, `skipped_by_state`, `tombstoned_count`, `coverage_complete`, `retry_count`, `http_status_counts`).
  - Added lineage tracking for `ingest.sync-all`.
  - Updated docs for batch ingestion, manifest schema, operational semantics, and troubleshooting.
- Added Ingestion Connectors v1:
  - New `honestroles ingest sync` command to fetch public ATS postings from Greenhouse, Lever, Ashby public postings, and Workable public endpoints.
  - Added deterministic ingestion pipeline with canonical parquet output, sync report artifact, optional raw JSONL, and incremental sync state.
  - Added new Python API surface `honestroles.sync_source(...) -> IngestionResult`.
  - Added deterministic dedup key precedence (`apply_url`/`job_url`, then `source+source_job_id`, then normalized fallback hash).
  - Added ingestion metadata columns (`source`, `source_ref`, `source_job_id`, `job_url`, `ingested_at_utc`, `source_payload_hash`) to normalized output.
  - Added lineage tracking for `ingest.sync` runs.
  - Added dedicated live smoke coverage for connectors (`tests/test_ingest_smoke_live.py`) and a separate manual GitHub Actions flow (`Ingestion Smoke`).
  - Updated CLI/docs with ingestion command, source-ref glossary, and ingestion troubleshooting guidance.
- Added Reliability Gate v1:
  - New `honestroles reliability check` command with policy-aware checks and artifact output.
  - Extended `honestroles doctor` with `--policy` and `--strict`.
  - Added stable reliability check codes and severities in check payloads.
  - Added reliability policy subsystem with built-in defaults and external `reliability.toml` loading.
  - Added shared reliability evaluator used by both `doctor` and `reliability check`.
- Extended lineage and run exploration:
  - Track `reliability.check` runs.
  - Added `check_codes` to run records.
  - Extended `honestroles runs list` with `--command`, `--since`, and `--contains-code`.
- Updated docs for reliability policy schema, strict-mode CI usage, and check-code remediation guidance.

## 0.1.4

- Added `honestroles init` to scaffold `pipeline.toml` and `plugins.toml` from a sample parquet, including adapter inference when available.
- Added `honestroles doctor` to validate environment, config, schema readiness, required content signals, and output path setup with actionable fixes.
- Added `honestroles runs list/show` for local run lineage exploration.
- Added unified CLI output formatting with `--format {json,table}` for structured-output commands, while keeping JSON as the default.
- Added run lineage persistence in `.honestroles/runs/<run_id>/run.json` for runtime, adapter, and EDA execution commands.
- Switched release publishing to manual deployment paths (local token script and manual GitHub workflow dispatch) using API token auth.

## 0.1.3

- Added fast pre-commit hygiene gates (trailing whitespace, EOF, docs refs/style/links) across all GitHub Actions workflows.
- Added shared `scripts/run_precommit_fast.sh` to centralize workflow pre-commit execution.
- Hardened CI/docs/fuzz/release workflows with fail-fast pre-commit checks before heavier jobs.

## 0.1.2

- Fixed `honestroles adapter infer` date parsing inference to avoid Polars timezone schema conflicts while scoring parseability.
- Fixed `honestroles eda dashboard` table rendering to avoid requiring `pyarrow` in test/runtime environments.
- Updated release workflow to publish with PyPI API token secrets (`PYPI_API_KEY` or `PYPI_API_TOKEN`) instead of Trusted Publishing OIDC.

## 0.1.1

- Added EDA v2 commands: `honestroles eda diff` and `honestroles eda gate`.
- Added EDA rules file support (`--rules-file`) with deterministic gate/drift thresholds.
- Added source-attributed EDA outputs: `quality.by_source`, `consistency.by_source`, and `findings_by_source`.
- Added diff artifacts (`artifact_kind=\"diff\"`) with drift metrics (numeric PSI, categorical JSD) and gate evaluation payload.
- Added `honestroles eda generate` for deterministic file-first EDA artifacts (`summary.json`, `report.md`, parquet tables, PNG figures).
- Added optional `honestroles eda dashboard` Streamlit wrapper that reads generated artifacts only (view layer, no profiling logic).
- Added pipeline-configurable input aliasing via `[input.aliases]` with diagnostics in `input_aliasing`.
- Added declarative source adapter layer via `[input.adapter]` with typed coercion, conflict tracking, and diagnostics in `input_adapter`.
- Added `honestroles adapter infer` to generate draft adapter TOML fragments and inference reports from parquet inputs.
- Replaced dataframe-first runtime/plugin contracts with typed domain objects: `PipelineSpec`, `JobDataset`, `PipelineRun`, `RuntimeDiagnostics`, `ApplicationPlanEntry`, and `PluginDefinition`.
- Added runtime quality profiles via `[runtime.quality]` and optional per-field weight overrides.
- Changed `report-quality`/`build_data_quality_report` semantics: `score_percent` is now weighted profile-based (not equal-weight mean-null).
- Added profile metadata in quality output: `profile`, `effective_weights`, `weighted_null_percent`.
- Removed legacy docs redirect pages and legacy script wrappers in favor of canonical docs IA and CLI commands.
- Raised minimum supported Python version to 3.11 and removed TOML loader compatibility fallback.

## 0.1.0

- Hard architectural rewrite to explicit runtime + manifest-driven plugin registry.
- Removed process-global plugin registration API.
- Migrated runtime data model to Polars-only.
- Added config-driven CLI (`honestroles`) with run/validate/report flows.
- Rebuilt deterministic and fuzz test suites around new runtime contracts.
