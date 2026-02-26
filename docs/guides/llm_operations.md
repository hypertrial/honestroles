# LLM Operations

## When to use this

Use this guide when enabling local Ollama-backed labeling, rating, or matching signals.

<div class="hr-callout">
  <strong>Choose this path if:</strong> you need deeper semantic enrichment and accept local model/runtime dependencies.
</div>

## Prerequisites

- Running local Ollama server
- Pulled model (for example, `llama3`)
- A deterministic baseline run for comparison

## Happy path

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

Expected output (success):

- LLM-related label/rating/match signal fields included where applicable

## Failure modes

- Ollama unreachable:
  - fix by starting server and verifying model availability
- Results too variable:
  - reduce scope to deterministic path for baseline comparison
- Long runtimes:
  - reduce batch size and evaluate fewer rows first

Failure example:

```text
ConnectionError: Failed to connect to Ollama endpoint
```

## Related pages

- [LLM Reference](../reference/llm.md)
- [Troubleshooting](troubleshooting.md)
- [End-to-End Pipeline](end_to_end_pipeline.md)
- [FAQ](../reference/faq.md)

<div class="hr-next-steps">
  <h2>Next actions</h2>
  <ul>
    <li>Compare LLM and deterministic outputs in <a href="end_to_end_pipeline.md">End-to-End Pipeline</a>.</li>
    <li>Use <a href="troubleshooting.md">Troubleshooting</a> when runtime availability is unstable.</li>
  </ul>
</div>
