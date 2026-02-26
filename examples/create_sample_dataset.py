from __future__ import annotations

import polars as pl


def main() -> None:
    pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "title": ["Data Engineer", "Senior ML Engineer", "Intern Analyst"],
            "company": ["A", "B", "C"],
            "location": ["Remote", "NYC", "Remote"],
            "remote": ["true", "false", "1"],
            "description_text": [
                "Python SQL data pipelines",
                "Build ML systems with Python and AWS",
                "Excel and reporting",
            ],
            "description_html": ["<p>Python SQL</p>", "<b>ML</b>", "<i>intern</i>"],
            "skills": ["python,sql", "python,aws", None],
            "salary_min": [120000, 180000, None],
            "salary_max": [160000, 220000, None],
            "apply_url": ["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            "posted_at": ["2026-01-01", "2026-01-02", "2026-01-03"],
        }
    ).write_parquet("examples/jobs_sample.parquet")


if __name__ == "__main__":
    main()
