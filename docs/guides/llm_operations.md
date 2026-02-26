# LLM Operations

## Purpose

This guide covers operational use of local LLM features in `honestroles` for labeling, quality scoring, and match-signal enrichment.

## Public API / Interface

LLM-enabled call sites:

- `label_jobs(..., use_llm=True, model=..., ollama_url=...)`
- `rate_jobs(..., use_llm=True, model=..., ollama_url=...)`
- `rank_jobs(..., use_llm_signals=True, model=..., ollama_url=...)`

Runtime requirement:

- local Ollama server reachable at `http://localhost:11434` by default

## Usage Example

```bash
ollama serve
ollama pull llama3
```

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.clean_jobs(df)

df = hr.label_jobs(df, use_llm=True, model="llama3", ollama_url="http://localhost:11434")
df = hr.rate_jobs(df, use_llm=True, model="llama3", ollama_url="http://localhost:11434")

profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=True, model="llama3")
```

## Edge Cases and Errors

- For deterministic baselines in CI, keep LLM features disabled.
- If Ollama is unavailable, LLM-specific columns may be absent or unchanged; check service health first.
- Use small batches first when testing new models to control latency.
- No additional package extra is required for current Ollama integration.

## Related Pages

- [LLM Reference](../reference/llm.md)
- [Troubleshooting](troubleshooting.md)
- [End-to-End Pipeline](end_to_end_pipeline.md)
- [Quickstart](../start/quickstart.md)
