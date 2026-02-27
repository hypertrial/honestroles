# EDA Rules Schema

Rules for `honestroles eda diff` and `honestroles eda gate` can be loaded from TOML and overridden by CLI flags.

## Precedence

1. CLI flags (`--fail-on`, `--warn-on`)
2. Rules file (`--rules-file`)
3. Built-in defaults

## Gate Rules

```toml
[gate]
fail_on = ["P0"]
warn_on = ["P1"]
max_p0 = 0
max_p1 = 999999
```

Fields:

- `fail_on`: severities that fail gate when count exceeds threshold (`P0`, `P1`, `P2`)
- `warn_on`: severities that surface warnings
- `max_p0`: max allowed `P0` findings before failure
- `max_p1`: max allowed `P1` findings before failure (if `P1` is in `fail_on`)

## Drift Rules

```toml
[drift]
numeric_warn_psi = 0.10
numeric_fail_psi = 0.25
categorical_warn_jsd = 0.10
categorical_fail_jsd = 0.20
columns_numeric = ["salary_min", "salary_max"]
columns_categorical = ["source", "remote", "location", "company"]
```

Fields:

- `numeric_warn_psi`, `numeric_fail_psi`: thresholds for numeric PSI drift
- `categorical_warn_jsd`, `categorical_fail_jsd`: thresholds for categorical JSD drift
- `columns_numeric`: numeric columns to evaluate
- `columns_categorical`: categorical columns to evaluate

## CLI Overrides

```bash
$ honestroles eda gate \
  --candidate-dir dist/eda/candidate \
  --baseline-dir dist/eda/baseline \
  --rules-file eda-rules.toml \
  --fail-on P0,P1 \
  --warn-on P2
```

`--fail-on` and `--warn-on` always override values from `--rules-file`.
