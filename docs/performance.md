# Performance Guardrails

`honestroles` includes runtime guardrail tests for core hot paths.

## Covered Paths

1. `clean_jobs`
2. `filter_jobs`
3. `rate_jobs` (non-LLM mode)

## Test Location

- `tests/test_perf/test_guardrails.py`

## Threshold Controls

Thresholds are configurable via environment variables:

- `HONESTROLES_MAX_CLEAN_SECONDS`
- `HONESTROLES_MAX_FILTER_SECONDS`
- `HONESTROLES_MAX_RATE_SECONDS`

These defaults are intentionally conservative and are tuned tighter in CI for regression detection.
