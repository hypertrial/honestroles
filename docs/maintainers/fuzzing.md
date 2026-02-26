# Fuzz Testing

`honestroles` uses Hypothesis-based fuzz/property tests to harden parser and normalization paths against malformed inputs.

## Run Commands

- PR-smoke profile:
  - `HYPOTHESIS_PROFILE=ci_smoke pytest -m "fuzz" -q -o addopts="-n auto"`
- Nightly-deep profile:
  - `HYPOTHESIS_PROFILE=nightly_deep pytest -m "fuzz" -q -o addopts="-n auto"`

The default test suite excludes fuzz tests via pytest `addopts` so normal contributor runs remain fast.

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

## Known-Bug Policy (`xfail`)

- Use `pytest.mark.xfail(strict=True)` for known regressions that fuzz tests should document immediately.
- Keep the reason concise and actionable.
- Remove `xfail` as soon as the bug is fixed; strict mode will fail on XPASS to enforce cleanup.

## Reproducing Counterexamples

1. Re-run the failing node directly:
   - `HYPOTHESIS_PROFILE=ci_smoke pytest <nodeid> -q -o addopts="-n auto"`
2. If the failure output includes a seed, replay it:
   - `HYPOTHESIS_SEED=<seed> pytest <nodeid> -q -o addopts="-n auto"`
3. Convert the minimized falsifying example into a deterministic regression test when practical.

