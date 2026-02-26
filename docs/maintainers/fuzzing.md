# Fuzz Testing

`honestroles` uses Hypothesis-based fuzz/property tests to harden parser and normalization paths against malformed inputs.

## Run Commands

- Full local smoke run:
  - `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q -o addopts="-n auto"`
- Full local deep run:
  - `HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q -o addopts="-n auto"`

## CI Shards

PR and nightly jobs are sharded to keep runtime stable as coverage grows.

- `clean_filter`
  - `tests/fuzz/test_fuzz_clean_*.py`
  - `tests/fuzz/test_fuzz_filter_*.py`
- `label_rate`
  - `tests/fuzz/test_fuzz_label_*.py`
  - `tests/fuzz/test_fuzz_rate_*.py`
- `io`
  - `tests/fuzz/test_fuzz_io_*.py`
  - `tests/fuzz/test_fuzz_duckdb_query_validation.py`
- `match`
  - `tests/fuzz/test_fuzz_match_*.py`
- `plugins_cli_llm`
  - `tests/fuzz/test_fuzz_plugins_*.py`
  - `tests/fuzz/test_fuzz_cli_*.py`
  - `tests/fuzz/test_fuzz_llm_*.py`
  - `tests/fuzz/test_fuzz_init_surface.py`

Shard-local smoke command pattern:

- `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q -o addopts="-n auto" <shard paths>`

## Profiles

- `ci_smoke`
  - lower `max_examples`
  - deterministic (`derandomize=True`)
  - intended for pull requests
- `nightly_deep`
  - higher `max_examples`
  - broader stochastic exploration
  - intended for scheduled jobs

Profiles are loaded from `HYPOTHESIS_PROFILE` in `tests/conftest.py`.

## Regression Policy

Fuzz tests are expected to stay green. Newly discovered regressions should be fixed in the same change whenever possible.

If immediate remediation is blocked:

1. File an issue with minimized reproducer and stack trace.
2. Add the smallest deterministic regression test possible.
3. Add temporary quarantine only with a clear owner and removal date.

## Reproducing Counterexamples

1. Re-run the failing node directly:
   - `HYPOTHESIS_PROFILE=ci_smoke pytest <nodeid> -q -o addopts="-n auto"`
2. If the failure output includes a seed, replay it:
   - `HYPOTHESIS_SEED=<seed> pytest <nodeid> -q -o addopts="-n auto"`
3. Re-run just the affected shard while iterating:
   - `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q -o addopts="-n auto" <shard paths>`
4. Convert the minimized falsifying example into a deterministic regression test when practical.
