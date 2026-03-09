# Configure Pipeline

Configure stage behavior and runtime policy using a strict TOML schema.

## When to use

Use this when changing filtering, scoring, ranking, or failure behavior.

## Prerequisites

- A valid pipeline file (see [First Pipeline Config](../getting-started/first-pipeline-config.md))

## Steps

1. Set input/output paths:

```toml
[input]
kind = "parquet"
path = "./examples/jobs_sample.parquet"

[output]
path = "./examples/jobs_scored.parquet"
```

2. Configure stages:

```toml
[stages.filter]
enabled = true
remote_only = true
min_salary = 120000.0
required_keywords = ["python", "sql"]

[stages.rate]
enabled = true
completeness_weight = 0.7
quality_weight = 0.3

[stages.match]
enabled = true
top_k = 25
```

3. Add source adapter mappings for non-canonical input schemas:

```toml
[input.adapter]
enabled = true
on_error = "null_warn"

[input.adapter.fields.location]
from = ["location_raw", "job_location"]
cast = "string"

[input.adapter.fields.remote]
from = ["remote_flag", "is_remote"]
cast = "bool"
```

4. Set runtime policy:

```toml
[runtime]
fail_fast = false
random_seed = 42
```

5. Validate before running:

```bash
$ honestroles config validate --pipeline pipeline.toml
```

6. Define reliability thresholds in a separate policy file (optional but recommended):

```toml
# reliability.toml
min_rows = 500
required_columns = ["title", "description_text", "posted_at"]

[max_null_pct]
title = 5
description_text = 10

[freshness]
column = "posted_at"
max_age_days = 14
```

Run:

```bash
$ honestroles reliability check --pipeline-config pipeline.toml --policy reliability.toml --strict --format table
```

## Expected result

Config validation succeeds and stage/runtime values appear in normalized JSON output.

## Next steps

- Full field-level schema: [Pipeline Config Schema](../reference/pipeline-config-schema.md)
- Reliability threshold schema: [Reliability Policy Schema](../reference/reliability-policy-schema.md)
- Error handling behavior: [Non-Fail-Fast and Recovery](non-fail-fast-and-recovery.md)
