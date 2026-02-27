# Stage Contracts

Canonical stage behavior and data contracts.

## Stage Order

Enabled stages execute in this fixed order:

1. `clean`
2. `filter`
3. `label`
4. `rate`
5. `match`

## Source Data Contract

Runtime normalizes columns to include these names:

- `id`
- `title`
- `company`
- `location`
- `remote`
- `description_text`
- `description_html`
- `skills`
- `salary_min`
- `salary_max`
- `apply_url`
- `posted_at`

Before normalization, runtime resolves source aliases into canonical names using:

- Built-in aliases: `location_raw -> location`, `remote_flag -> remote`
- Optional pipeline aliases from `[input.aliases]`

Conflict policy:

- Canonical field wins.
- Conflicts with alias values are recorded in diagnostics.

Validation requires:

- `title`
- At least one of `description_text` or `description_html`

## Stage Responsibilities

- `clean`: normalize/cast schema, clean text/html, normalize booleans/lists, optionally drop null titles
- `filter`: apply `remote_only`, salary threshold, keyword filters, then run filter plugins
- `label`: derive base labels, then run label plugins
- `rate`: compute bounded `rate_completeness`, `rate_quality`, and `rate_composite`, then run rate plugins
- `match`: compute bounded `fit_score`, sort descending, enforce `top_k`, generate `application_plan`

## Output Invariants

- `rate_*` metrics are bounded to `[0.0, 1.0]`.
- `fit_score` is bounded to `[0.0, 1.0]`.
- `fit_rank` starts at `1` and reflects descending `fit_score`.
- `application_plan` is aligned to ranked top `top_k` rows.

## Diagnostics Additions

Runtime diagnostics include `input_aliasing`:

```json
{
  "input_aliasing": {
    "applied": {"location": "location_raw", "remote": "remote_flag"},
    "conflicts": {"remote": 2},
    "unresolved": ["skills", "salary_min", "salary_max"]
  }
}
```
