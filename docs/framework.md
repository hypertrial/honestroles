---
title: Framework Boundary
description: Canonical contract separating user-controlled customization from framework-enforced runtime mechanics.
---

# Framework vs User Control

This document is the canonical execution contract for HonestRoles.

## Framework (Non-Negotiable)

1. Explicit runtime boundary:
   - All pipeline execution goes through `HonestRolesRuntime`.
   - Runtime is constructed from TOML config and an optional plugin manifest.
2. Deterministic execution model:
   - The data model is `polars.DataFrame` end-to-end.
   - Enabled stages run in a fixed order: `clean -> filter -> label -> rate -> match`.
   - `random_seed` is applied at run start; fixed inputs/config/plugins produce stable outputs.
3. Strict config validation:
   - Pipeline and plugin manifest models are strict (`extra="forbid"`).
   - Relative pipeline paths are resolved against the pipeline config directory.
   - `match.top_k` must be `>= 1`; rate weights must be non-negative.
4. Source data contract enforcement:
   - Runtime normalizes/casts into a canonical job schema.
   - Validation requires `title` and at least one of `description_text` or `description_html`.
5. Manifest-driven plugin loading only:
   - No process-global plugin registration API.
   - Plugins are loaded from `[[plugins]]`, filtered by `enabled`, and ordered deterministically by `(kind, order, name)`.
6. Plugin ABI enforcement:
   - Callable references must use `module:function`.
   - Signatures are validated by kind: `(pl.DataFrame, *PluginContext) -> pl.DataFrame`.
   - Plugin settings are deep-frozen (immutable mapping/tuple/frozenset forms).
7. Typed, explicit failure semantics:
   - Stage failures surface as `StageExecutionError`.
   - Plugin import/validation/execution failures surface as plugin-specific errors.
   - `fail_fast=true` raises immediately; `fail_fast=false` records non-fatal stage errors and continues.
8. Output invariants:
   - `rate_*` metrics and `fit_score` are bounded to `[0.0, 1.0]`.
   - `match` always emits ranked results plus `application_plan` for top `top_k` rows.
   - Diagnostics always capture stage row counts, plugin counts, runtime settings, and final row count.

## User Owns (Flexible)

1. Pipeline intent in `pipeline.toml`:
   - Stage enable/disable choices.
   - Stage options (`remote_only`, keywords, salary floors, weights, `top_k`, etc.).
   - Runtime options (`fail_fast`, `random_seed`) and optional output path.
2. Plugin implementation:
   - Custom filter/label/rate logic inside ABI-compliant callables.
   - Plugin packaging and module layout.
3. Plugin composition:
   - Which plugins are enabled.
   - Per-stage ordering and per-plugin settings in `plugins.toml`.
4. Operator interface:
   - Use the CLI (`run`, `validate`, `report-quality`, `scaffold-plugin`) or the runtime API directly.

## Handoff Boundary

Users provide:

1. A pipeline config.
2. An optional plugin manifest.
3. Plugin callables that satisfy ABI contracts.

The framework owns everything after handoff:

1. Config parsing, validation, and path resolution.
2. Plugin importing, signature/type checks, and deterministic ordering.
3. Stage orchestration and plugin context injection.
4. Error wrapping, diagnostics collection, ranking, and optional Parquet output.

The user never writes or replaces the runtime execution loop.

## Required Behavior

1. Fixed inputs/config/plugins produce repeatable results.
2. Plugin execution is instance-scoped; registries do not leak across runtimes.
3. Plugin settings and runtime context are read-only from plugin code.
4. Runtime behavior is consistent between library usage and CLI wrappers.
5. Failure paths are explicit, typed, and include stage/plugin context.

## Production Run Lifecycle

1. Load and validate `pipeline.toml` (resolve relative paths).
2. Optionally load and validate `plugins.toml` into an immutable ordered registry.
3. Initialize runtime diagnostics and deterministic seed.
4. Read Parquet input, normalize schema, and validate source contract.
5. Execute enabled stages in fixed order; run same-kind plugins in deterministic order.
6. Generate ranked `fit_score` output and `application_plan` in `match`.
7. Optionally write output Parquet and return `RuntimeResult`.
