# DuckDB Source Schema

This document defines the schema for the DuckDB database used by the scraper. It serves as the source-of-truth for downstream transformations.

## Tables


### `companies_discovered`
Stores companies discovered via crawling (e.g., Common Crawl), identifying potential ATS endpoints.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `ats_type` | `VARCHAR` | `PK` | Type of ATS detected |
| `ats_org` | `VARCHAR` | `PK` | ATS organization slug detected |
| `company_name` | `VARCHAR` | | Extracted company name |
| `company_slug` | `VARCHAR` | | Slugified company name |
| `company_name_source` | `VARCHAR` | | Source of the company name |
| `board_url` | `VARCHAR` | | URL where the board was found |
| `company_domain` | `VARCHAR` | | Domain of the company |
| `first_seen` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | First time seen |
| `last_seen` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | Last time seen |
| `last_enriched_at` | `TIMESTAMP` | | Timestamp of last enrichment |

### `jobs_current`
Stores the current state of active jobs.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `job_key` | `VARCHAR` | `PRIMARY KEY` | Unique job identifier (`company::source::job_id`) |
| `company` | `VARCHAR` | `NOT NULL` | Company identifier |
| `source` | `VARCHAR` | `NOT NULL` | ATS Source (e.g., greenhouse) |
| `job_id` | `VARCHAR` | `NOT NULL` | ID from the ATS |
| `title` | `VARCHAR` | `NOT NULL` | Job title |
| `team` | `VARCHAR` | | Team or department name |
| `location_raw` | `VARCHAR` | `NOT NULL` | Raw location string |
| `remote_flag` | `BOOLEAN` | `DEFAULT FALSE` | Simple remote flag |
| `employment_type` | `VARCHAR` | | Employment type (e.g., full-time) |
| `posted_at` | `TIMESTAMP` | | Date posted |
| `updated_at` | `TIMESTAMP` | | Date updated |
| `apply_url` | `VARCHAR` | `NOT NULL` | URL to apply |
| `description_html` | `TEXT` | | HTML description |
| `description_text` | `TEXT` | | Plain text description |
| `ingested_at` | `TIMESTAMP` | `NOT NULL` | Ingestion timestamp |
| `content_hash` | `VARCHAR` | `NOT NULL` | Hash of stable content content |
| `salary_min` | `DOUBLE` | | Minimum salary |
| `salary_max` | `DOUBLE` | | Maximum salary |
| `salary_currency` | `VARCHAR` | | Salary currency |
| `salary_interval` | `VARCHAR` | | Salary interval |
| `city` | `VARCHAR` | | Enriched city |
| `country` | `VARCHAR` | | Enriched country |
| `remote_type` | `VARCHAR` | | Enriched remote type |
| `skills` | `VARCHAR[]` | | Extracted skills |
| `last_seen` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | Last time seen already processed |
| `salary_text` | `VARCHAR` | | Raw salary string |
| `languages` | `VARCHAR[]` | | Extracted languages requirements |
| `benefits` | `VARCHAR[]` | | Extracted benefits |
| `visa_sponsorship` | `BOOLEAN` | | Visa sponsorship mentioned |

### `jobs_historical`
Stores historical snapshots of jobs, including Common Crawl provenance.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | `PRIMARY KEY` | Auto-incrementing ID |
| `job_key` | `VARCHAR` | `NOT NULL` | Job identifier |
| `crawl_id` | `VARCHAR` | | Common Crawl ID |
| `crawl_timestamp` | `TIMESTAMP` | | Common Crawl timestamp |
| `crawl_url` | `VARCHAR` | | URL processed by crawler |
| `source_url` | `VARCHAR` | | Source URL for validation |
| *... (Inherits all columns from `jobs_current`)* | | | |

### `job_events`
Tracks lifecycle events for jobs.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `INTEGER` | `PRIMARY KEY` | Auto-incrementing ID |
| `job_key` | `VARCHAR` | `NOT NULL` | Job identifier |
| `event_type` | `VARCHAR` | `NOT NULL` | 'opened', 'closed', 'changed' |
| `event_date` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | Event timestamp |
| `old_hash` | `VARCHAR` | | Previous state hash |
| `new_hash` | `VARCHAR` | | New state hash |

### `crawl_completions`
Tracks progress of historical crawls.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `crawl_id` | `VARCHAR` | `PRIMARY KEY` | Common Crawl snapshot ID |
| `completed_at` | `TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | Completion time |
| `ats_types_processed` | `VARCHAR` | | List of ATS types handled |
| `total_slugs_found` | `INTEGER` | `DEFAULT 0` | Metrics |
