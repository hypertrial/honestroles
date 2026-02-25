## Clean

`honestroles.clean` provides a lightweight cleaning pipeline for job postings,
including HTML stripping, location normalization, salary parsing, and deduping.

### Modules

- `__init__.py`: `clean_jobs` orchestrator and re-exports.
- `dedup.py`: row-level deduplication by content hash or custom columns.
- `historical.py`: opt-in historical cleaning and snapshot compaction.
- `html.py`: HTML to text conversion and boilerplate removal.
- `normalize.py`: location, salary, skills, and employment type normalization.
- `location_data.py`: static lookup tables used by location normalization.

### Public API reference

#### `clean_jobs(df: pd.DataFrame) -> pd.DataFrame`

Runs the standard cleaning pipeline in order:
`strip_html` -> `normalize_locations` -> `enrich_country_from_context` ->
`normalize_salaries` -> `normalize_skills` -> `normalize_employment_types` ->
`deduplicate`.

#### `clean_historical_jobs(df: pd.DataFrame, *, options: HistoricalCleanOptions | None = None) -> pd.DataFrame`

Runs the historical cleaning flow:
`normalize_source_data_contract` -> listing-page detection/drop -> snapshot compaction ->
standard cleaning steps (`strip_html`, `normalize_locations`, `enrich_country_from_context`,
`normalize_salaries`, `normalize_skills`, `normalize_employment_types`).

Adds:
- `historical_is_listing_page`
- `snapshot_count` / `first_seen` / `last_seen` (when compaction enabled)
  - default historical mode emits timezone-aware UTC datetimes for
    `first_seen` / `last_seen`
  - set `snapshot_timestamp_output="iso8601"` to emit legacy ISO-8601 strings

#### `HistoricalCleanOptions`

Dataclass for historical mode behavior:
- `detect_listing_pages: bool = True`
- `drop_listing_pages: bool = True`
- `compact_snapshots: bool = True`
- `prefer_existing_description_text: bool = True`
- `snapshot_timestamp_output: Literal["iso8601", "datetime"] = "datetime"`
- `compaction_keys: tuple[str, ...] = ("job_key", "content_hash")`
- `ingested_at_column: str = "ingested_at"`

#### `detect_historical_listing_pages(df: pd.DataFrame, ...) -> pd.Series`

Marks likely listing/landing rows using:
- `location_raw == "Unknown"`
- `title` ending in `" jobs"`
- slug-like `job_id` (`^[a-z0-9-]{3,40}$`)

#### `deduplicate(df: pd.DataFrame, *, subset: list[str] | None = None, keep: str = "first") -> pd.DataFrame`

Drops duplicate rows. Defaults to `CONTENT_HASH` when present. Uses `keep` with
`pandas.DataFrame.drop_duplicates`.

#### `strip_html(df: pd.DataFrame, *, html_column: str = DESCRIPTION_HTML, text_column: str = DESCRIPTION_TEXT) -> pd.DataFrame`

Converts HTML in `html_column` to plain text, removes common boilerplate lines,
and writes to `text_column`. Logs a warning if the HTML column is missing.

#### `normalize_locations(...) -> pd.DataFrame`

```
normalize_locations(
    df: pd.DataFrame,
    *,
    location_column: str = LOCATION_RAW,
    city_column: str = CITY,
    region_column: str = REGION,
    country_column: str = COUNTRY,
    remote_flag_column: str = REMOTE_FLAG,
    remote_type_column: str = REMOTE_TYPE,
) -> pd.DataFrame
```

Parses `location_raw` into `city`, `region`, `country`, and `remote_type`,
accounting for common aliases, multi-location strings, unknown-token cleanup,
and remote inference from title/description and optional `remote_allowed`.

#### `enrich_country_from_context(...) -> pd.DataFrame`

```
enrich_country_from_context(
    df: pd.DataFrame,
    *,
    country_column: str = COUNTRY,
    region_column: str = REGION,
    description_column: str = DESCRIPTION_TEXT,
    title_column: str = TITLE,
    salary_text_column: str = SALARY_TEXT,
    salary_currency_column: str = SALARY_CURRENCY,
    apply_url_column: str = APPLY_URL,
    benefits_column: str = BENEFITS,
) -> pd.DataFrame
```

Infers missing country/region using signals in text, salary currency, and URLs.
Currently includes Canada-specific enrichment signals.

#### `normalize_salaries(...) -> pd.DataFrame`

```
normalize_salaries(
    df: pd.DataFrame,
    *,
    salary_text_column: str = SALARY_TEXT,
    salary_min_column: str = SALARY_MIN,
    salary_max_column: str = SALARY_MAX,
    salary_currency_column: str = SALARY_CURRENCY,
    salary_interval_column: str = SALARY_INTERVAL,
) -> pd.DataFrame
```

Parses salary ranges (and conservative single-value salary statements) from
`salary_text`, with fallback to `description_text` on salary-like rows, and
writes `salary_min`/`salary_max`. Defaults currency to `USD` and interval to
`year` when missing.

#### `normalize_skills(...) -> pd.DataFrame`

```
normalize_skills(
    df: pd.DataFrame,
    *,
    skills_column: str = SKILLS,
    title_column: str = TITLE,
    description_column: str = DESCRIPTION_TEXT,
) -> pd.DataFrame
```

Backfills missing/empty `skills` deterministically from title+description using
alias-normalized dictionary matching while preserving existing non-empty skills.

#### `normalize_employment_types(df: pd.DataFrame, *, employment_type_column: str = EMPLOYMENT_TYPE) -> pd.DataFrame`

Normalizes free-form employment type values (e.g., "Full Time" -> `full_time`).

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
```

```python
from honestroles.clean import clean_historical_jobs

historical = hr.read_parquet("jobs_historical.parquet", validate=False)
historical = clean_historical_jobs(historical)
```

```python
from honestroles.clean import normalize_locations, normalize_salaries

df = normalize_locations(df)
df = normalize_salaries(df)
```

### Design notes

- `location_data.py` contains the static maps and keyword lists used by
  normalization (country aliases, region aliases, remote keywords, and
  Canada-specific signals and city/province lookups).
- `LocationResult` (a frozen dataclass) is used internally by
  `normalize_locations` to make parsing decisions explicit.
- Boilerplate removal in `strip_html` uses regex patterns in
  `_BOILERPLATE_PATTERNS` to drop common EEO/legal lines.
