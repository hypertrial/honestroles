## Clean

`honestroles.clean` provides a lightweight cleaning pipeline for job postings,
including HTML stripping, location normalization, salary parsing, and deduping.

### Modules

- `__init__.py`: `clean_jobs` orchestrator and re-exports.
- `dedup.py`: row-level deduplication by content hash or custom columns.
- `html.py`: HTML to text conversion and boilerplate removal.
- `normalize.py`: location, salary, and employment type normalization.
- `location_data.py`: static lookup tables used by location normalization.

### Public API reference

#### `clean_jobs(df: pd.DataFrame) -> pd.DataFrame`

Runs the standard cleaning pipeline in order:
`strip_html` -> `normalize_locations` -> `enrich_country_from_context` ->
`normalize_salaries` -> `normalize_employment_types` -> `deduplicate`.

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
accounting for common aliases, multi-location strings, and remote keywords.

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

Parses salary ranges from text and writes `salary_min`/`salary_max`. Defaults
currency to `USD` and interval to `year` when missing.

#### `normalize_employment_types(df: pd.DataFrame, *, employment_type_column: str = EMPLOYMENT_TYPE) -> pd.DataFrame`

Normalizes free-form employment type values (e.g., "Full Time" -> `full_time`).

### Usage examples

```python
import honestroles as hr

df = hr.read_parquet("jobs_current.parquet")
df = hr.clean_jobs(df)
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
