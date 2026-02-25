# Performance Guardrails

`honestroles` includes runtime guardrail tests for core hot paths.

## Covered Paths

1. `clean_jobs`
2. `filter_jobs`
3. `rate_jobs` (non-LLM mode)
4. `clean_historical_jobs` speedup vs ISO8601 timestamp mode
5. Historical non-LLM quality coverage guardrail (`jobs_historical.parquet`)
6. `validate_source_data_contract` (strict mode)

## Test Location

- `tests/test_perf/test_guardrails.py`
- `tests/test_perf/test_historical_clean_guardrail.py`
- `tests/test_perf/test_historical_quality_guardrail.py`
- `tests/test_perf/test_contract_validation_guardrail.py`

## Threshold Controls

Thresholds are configurable via environment variables:

- `HONESTROLES_MAX_CLEAN_SECONDS`
- `HONESTROLES_MAX_FILTER_SECONDS`
- `HONESTROLES_MAX_RATE_SECONDS`
- `HONESTROLES_MAX_HISTORICAL_CLEAN_SECONDS`
- `HONESTROLES_MIN_HISTORICAL_CLEAN_SPEEDUP`
- `HONESTROLES_QUALITY_CLEAN_ROWS`
- `HONESTROLES_QUALITY_SIGNAL_ROWS`
- `HONESTROLES_MIN_CLEAN_SKILLS_COVERAGE`
- `HONESTROLES_MIN_CLEAN_SALARY_COVERAGE`
- `HONESTROLES_MIN_CLEAN_REMOTE_COVERAGE`
- `HONESTROLES_MIN_REQUIRED_SKILLS_COVERAGE`
- `HONESTROLES_MIN_EXPERIENCE_YEARS_COVERAGE`
- `HONESTROLES_MAX_PM_FALSE_POSITIVE_RATIO`
- `HONESTROLES_MAX_CONTRACT_VALIDATE_SECONDS`
- `HONESTROLES_MIN_CONTRACT_SPEEDUP`

These defaults are intentionally conservative and are tuned tighter in CI for regression detection.
