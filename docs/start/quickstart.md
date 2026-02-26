# Quickstart

Create a pipeline config:

```toml
[input]
kind = "parquet"
path = "./jobs.parquet"

[output]
path = "./jobs_scored.parquet"

[stages.clean]
enabled = true

[stages.filter]
enabled = true
remote_only = true
required_keywords = ["python"]

[stages.label]
enabled = true

[stages.rate]
enabled = true

[stages.match]
enabled = true
top_k = 100
```

Optional plugin manifest:

```toml
[[plugins]]
name = "label_note"
kind = "label"
callable = "my_plugins.labeling:annotate"
enabled = true
order = 10
```

Run:

```bash
honestroles run --pipeline-config pipeline.toml --plugins plugins.toml
```
