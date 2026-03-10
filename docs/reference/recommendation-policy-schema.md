# Recommendation Policy Schema

Reference for `recommendation.toml` used by:

- `honestroles recommend build-index --policy recommendation.toml`
- `honestroles recommend match --policy recommendation.toml`
- `honestroles recommend evaluate --policy recommendation.toml`

If omitted, HonestRoles uses the built-in default scoring policy.

## Schema

```toml
[weights]
skills = 0.35
title = 0.20
location_work_mode = 0.15
seniority = 0.10
recency = 0.10
compensation = 0.10

reason_limit = 3
```

## Fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `weights.skills` | float >= 0 | `0.35` | Skill-overlap signal weight |
| `weights.title` | float >= 0 | `0.20` | Title-similarity signal weight |
| `weights.location_work_mode` | float >= 0 | `0.15` | Location/work-mode fit weight |
| `weights.seniority` | float >= 0 | `0.10` | Seniority-fit weight |
| `weights.recency` | float >= 0 | `0.10` | Posting recency weight |
| `weights.compensation` | float >= 0 | `0.10` | Compensation-fit weight |
| `reason_limit` | integer >= 1 | `3` | Number of top `match_reasons` returned |

Weights are normalized to sum to 1 at runtime.
