# Recommendation Eval Thresholds

Reference for `recommend_eval.toml` used by:

- `honestroles recommend evaluate --thresholds recommend_eval.toml`

If omitted, HonestRoles uses built-in defaults.

## Schema

```toml
ks = [10, 25, 50]
precision_at_10_min = 0.60
recall_at_25_min = 0.70
```

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `ks` | array[int>=1] | `[10, 25, 50]` | `k` values used for precision/recall metrics |
| `precision_at_10_min` | float in [0,1] | `0.60` | Required floor for `precision_at_10` |
| `recall_at_25_min` | float in [0,1] | `0.70` | Required floor for `recall_at_25` |

## Exit Behavior

- `pass` -> exit `0`
- threshold failure -> exit `1`
- invalid inputs/config -> exit `2`
