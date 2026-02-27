# Explore Data with EDA

Generate deterministic EDA artifacts from parquet inputs, compare runs, and enforce CI gate policies.

## When to use

Use this guide when you need reproducible profile/diff artifacts for PR review and automated quality checks.

## Prerequisites

- HonestRoles installed
- For dashboard mode: `pip install "honestroles[eda]"`

## Steps

Generate baseline and candidate artifacts:

```bash
$ honestroles eda generate --input-parquet jobs_baseline.parquet --output-dir dist/eda/baseline
$ honestroles eda generate --input-parquet jobs_candidate.parquet --output-dir dist/eda/candidate
```

Create diff artifacts:

```bash
$ honestroles eda diff --baseline-dir dist/eda/baseline --candidate-dir dist/eda/candidate --output-dir dist/eda/diff
```

Evaluate gate policy (CI-friendly):

```bash
$ honestroles eda gate --candidate-dir dist/eda/candidate --baseline-dir dist/eda/baseline --rules-file eda-rules.toml
```

Review artifacts:

```bash
$ cat dist/eda/candidate/report.md
$ cat dist/eda/candidate/summary.json
$ cat dist/eda/diff/diff.json
```

Launch dashboard view layer:

```bash
$ honestroles eda dashboard --artifacts-dir dist/eda/candidate --diff-dir dist/eda/diff --host 127.0.0.1 --port 8501
```

## Expected result

- `dist/eda/candidate` contains profile artifacts (`manifest.json`, `summary.json`, `report.md`, `tables/`, `figures/`)
- `dist/eda/diff` contains diff artifacts (`manifest.json`, `diff.json`, `tables/`)
- `eda gate` exits `0` on pass and `1` on policy failure

## Next steps

- Tune thresholds in `eda-rules.toml` for your CI bar.
- Track `diff.json` deltas over time to catch regressions early.
