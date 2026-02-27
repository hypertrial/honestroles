# First Pipeline Config

Use this page to understand the minimum valid `pipeline config`.

## When to use

Use this before customizing stage logic or runtime behavior.

## Prerequisites

- You completed [Quickstart (First Run)](quickstart-first-run.md)

## Steps

Start from this baseline:

```toml
[input]
kind = "parquet"
path = "./examples/jobs_sample.parquet"

[input.adapter]
enabled = true
on_error = "null_warn"

[output]
path = "./examples/jobs_scored.parquet"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = false

[stages.label]
enabled = true

[stages.rate]
enabled = true
completeness_weight = 0.5
quality_weight = 0.5

[stages.match]
enabled = true
top_k = 50

[runtime]
fail_fast = true
random_seed = 0
```

Validate it:

```bash
$ honestroles config validate --pipeline examples/sample_pipeline.toml
```

## Expected result

Validation returns exit code `0` and prints normalized JSON.

## Next steps

- Deep config options: [Pipeline Config Schema](../reference/pipeline-config-schema.md)
- Runtime behavior details: [Stage Contracts](../reference/stage-contracts.md)
