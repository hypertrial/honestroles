## Examples

These examples show how to run `honestroles` on local parquet data.

### Requirements

Install dependencies from the repo root:

```bash
pip install -e ".[dev]"
```

### Minimal pipeline

```bash
python examples/run_parquet.py "jobs_current.parquet" "jobs_scored.parquet"
```

The script reads the input parquet, cleans, filters, labels, and rates the data,
then writes the output parquet.

### Full pipeline (ranking + next actions)

```bash
python examples/shortlist_mds_new_grad.py "jobs_current.parquet" "jobs_shortlist.parquet" --top-n 50
```

The script cleans, labels, rates, ranks, and adds next actions for a Master's in Data Science new-grad profile.

### Built-in CLI helpers

```bash
honestroles-report-quality jobs_current.parquet --format text
honestroles-scaffold-plugin --name honestroles-plugin-myorg --output-dir .
```
