# Performance Guardrails

`honestroles` includes runtime guardrail tests for core hot paths.

## Covered Paths

1. `clean_jobs`
2. `filter_jobs`
3. `rate_jobs` (non-LLM mode)
4. `validate_source_data_contract` (strict mode)

## Test Location

- `tests/test_perf/test_guardrails.py`
- `tests/test_perf/test_contract_validation_guardrail.py`

## Threshold Controls

Thresholds are configurable via environment variables:

- `HONESTROLES_MAX_CLEAN_SECONDS`
- `HONESTROLES_MAX_FILTER_SECONDS`
- `HONESTROLES_MAX_RATE_SECONDS`
- `HONESTROLES_MAX_CONTRACT_VALIDATE_SECONDS`
- `HONESTROLES_MIN_CONTRACT_SPEEDUP`

These defaults are intentionally conservative and are tuned tighter in CI for regression detection.
