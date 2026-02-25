## Match

`honestroles.match` provides agent-first profile matching, ranking, and action planning.

### Public API

#### `CandidateProfile`

Dataclass describing candidate constraints and preferences. Use `CandidateProfile.mds_new_grad()` for recommended defaults for Master's in Data Science new grads.

#### `extract_job_signals(...) -> pd.DataFrame`

Extracts matching signals from job text (required/preferred skills, experience
requirements, entry-level likelihood, sponsorship signal, clarity, friction).
Uses a deterministic non-LLM path first (vectorized/precomputed series with
source-field precedence for experience and visa), then optional Ollama
enrichment for low-confidence rows.

#### `rank_jobs(...) -> pd.DataFrame`

Ranks jobs for a profile and returns:

- `fit_score`
- `fit_breakdown`
- `missing_requirements`
- `why_match`

`rank_jobs` also supports microproduct extension hooks via `component_overrides` so builders can replace built-in component scorers (for example, a custom salary model).

#### `build_application_plan(...) -> pd.DataFrame`

Adds `next_actions` recommendations for top-ranked jobs.

### Example

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet", validate=False)
df = hr.clean_jobs(df)
df = hr.label_jobs(df, use_llm=False)
df = hr.rate_jobs(df, use_llm=False)

profile = hr.CandidateProfile.mds_new_grad()
ranked = hr.rank_jobs(df, profile=profile, use_llm_signals=False, top_n=50)
plan = hr.build_application_plan(ranked, profile=profile, top_n=20)
```
