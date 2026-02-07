from __future__ import annotations

import argparse
from pathlib import Path
import re

import pandas as pd

import honestroles as hr
from honestroles.clean.location_data import (
    CANADIAN_BENEFIT_KEYWORDS,
    CANADIAN_COMPLIANCE_KEYWORDS,
    CANADIAN_COUNTRY_KEYWORDS,
    CANADIAN_CURRENCY_KEYWORDS,
    CANADIAN_POSTAL_RE_PATTERN,
    COUNTRY_ALIASES,
    REGION_ALIASES,
    US_CITIES,
)
from honestroles.clean.normalize import enrich_country_from_context, normalize_locations

_CANADIAN_POSTAL_RE = re.compile(CANADIAN_POSTAL_RE_PATTERN, re.IGNORECASE)
_CANADA_CONTEXT_RE = re.compile(
    r"(?:based in|located in|position in|role in|work in|working in|must be in|"
    r"reside in|within|across|eligible to work in|authorized to work in|remote in|"
    r"remote within)\s+(?:the\s+)?(?:canada|canadian)",
    re.IGNORECASE,
)
_CANADA_BASED_RE = re.compile(r"(?:canada|canadian)[- ]based", re.IGNORECASE)


def _normalize_country(token: str | None) -> str | None:
    if not token:
        return None
    normalized = token.strip().lower()
    return COUNTRY_ALIASES.get(normalized)


def _baseline_split(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    parts = [part.strip() for part in str(value).split(",") if part.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return None, None


def _us_state_aliases() -> set[str]:
    return {alias for alias, (_, country) in REGION_ALIASES.items() if country == "US"}


def _looks_us_location(value: str | None) -> bool:
    if not value:
        return False
    normalized = str(value).lower()
    if "united states" in normalized or "usa" in normalized or " u.s." in normalized:
        return True
    tokens = [
        token.strip()
        for token in normalized.replace("|", ",").replace("/", ",").split(",")
        if token.strip()
    ]
    state_aliases = _us_state_aliases()
    for token in tokens:
        if token in US_CITIES:
            return True
        if token in state_aliases:
            return True
    return False


def _collect_text(row: pd.Series, columns: list[str]) -> str:
    parts: list[str] = []
    for column in columns:
        if column not in row:
            continue
        value = row[column]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        else:
            parts.append(str(value))
    return " ".join(parts).lower()


def _canadian_signals(row: pd.Series) -> list[str]:
    text = _collect_text(
        row,
        [
            "description_text",
            "title",
            "salary_text",
            "benefits",
            "apply_url",
        ],
    )
    signals: list[str] = []
    has_postal = _CANADIAN_POSTAL_RE.search(text) is not None
    if has_postal:
        signals.append("postal_code")
    has_country_keyword = any(keyword in text for keyword in CANADIAN_COUNTRY_KEYWORDS)
    has_context = has_country_keyword and (
        _CANADA_CONTEXT_RE.search(text) is not None
        or _CANADA_BASED_RE.search(text) is not None
    )
    if has_context:
        signals.append("country_keyword")
    has_currency_keyword = any(keyword in text for keyword in CANADIAN_CURRENCY_KEYWORDS)
    if has_currency_keyword:
        signals.append("currency_keyword")
    has_benefit = any(keyword in text for keyword in CANADIAN_BENEFIT_KEYWORDS)
    has_compliance = any(keyword in text for keyword in CANADIAN_COMPLIANCE_KEYWORDS)
    currency = row.get("salary_currency")
    if isinstance(currency, str) and currency.strip().upper() == "CAD":
        signals.append("salary_currency")
        has_currency_keyword = True
    if ".ca" in str(row.get("apply_url", "")).lower():
        signals.append("ca_tld")
    if has_context:
        if has_benefit:
            signals.append("benefit_keyword")
        if has_compliance:
            signals.append("compliance_keyword")
    return signals


def run(input_path: Path, *, limit: int, output_csv: Path | None) -> None:
    print(f"Loading parquet from {input_path}...")
    df = hr.read_parquet(input_path, validate=False)
    print(f"Loaded {len(df)} rows.")

    base_city: list[str | None] = []
    base_country_raw: list[str | None] = []
    for value in df.get("location_raw", pd.Series([None] * len(df))).tolist():
        city, country = _baseline_split(value)
        base_city.append(city)
        base_country_raw.append(country)
    baseline = df.copy()
    baseline["baseline_city"] = base_city
    baseline["baseline_country"] = [
        _normalize_country(country) for country in base_country_raw
    ]

    improved = normalize_locations(df)
    improved_country = improved["country"].fillna("")
    baseline_country = baseline["baseline_country"].fillna("")

    improved_canada = improved_country.eq("CA")
    baseline_canada = baseline_country.eq("CA")
    newly_canada = improved_canada & ~baseline_canada

    print("\nCanada identification summary:")
    print(f"- Baseline Canada count: {int(baseline_canada.sum())}")
    print(f"- Improved Canada count: {int(improved_canada.sum())}")
    print(f"- Newly identified as Canada: {int(newly_canada.sum())}")

    if improved_canada.any():
        print("\nImproved Canada breakdown by region:")
        improved_region_counts = (
            improved.loc[improved_canada, "region"].fillna("Unknown").value_counts()
        )
        for region, count in improved_region_counts.items():
            print(f"- {region}: {count}")

        print("\nBaseline vs improved Canada counts by region:")
        baseline_region_counts = (
            improved.loc[baseline_canada, "region"].fillna("Unknown").value_counts()
        )
        all_regions = sorted(
            set(improved_region_counts.index).union(baseline_region_counts.index)
        )
        for region in all_regions:
            baseline_count = int(baseline_region_counts.get(region, 0))
            improved_count = int(improved_region_counts.get(region, 0))
            print(f"- {region}: baseline={baseline_count}, improved={improved_count}")

        unknown_ca = improved_canada & improved["region"].isna()
        if unknown_ca.any():
            print("\nTop location_raw values for country=CA with unknown region:")
            top_unknown = (
                improved.loc[unknown_ca, "location_raw"]
                .fillna("")
                .value_counts()
                .head(limit)
            )
            for location, count in top_unknown.items():
                print(f"- {location}: {count}")

            likely_us = improved.loc[unknown_ca, "location_raw"].apply(
                _looks_us_location
            )
            print(
                "\nLikely US false positives (country=CA, region unknown): "
                f"{int(likely_us.sum())} of {int(unknown_ca.sum())}"
            )

    if newly_canada.any():
        sample = improved.loc[newly_canada].copy()
        sample = sample.assign(
            baseline_country=baseline.loc[newly_canada, "baseline_country"].values,
            baseline_city=baseline.loc[newly_canada, "baseline_city"].values,
        )
        cols = [
            "job_key",
            "company",
            "title",
            "location_raw",
            "baseline_city",
            "baseline_country",
            "city",
            "region",
            "country",
            "remote_type",
        ]
        available = [col for col in cols if col in sample.columns]
        print(f"\nSample of newly identified Canada jobs (up to {limit}):")
        print(sample[available].head(limit).to_string(index=False))

        print("\nTop newly-identified location_raw values:")
        top_locations = sample["location_raw"].fillna("").value_counts().head(limit)
        for location, count in top_locations.items():
            print(f"- {location}: {count}")

        if output_csv is not None:
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            sample[available].to_csv(output_csv, index=False)
            print(f"\nWrote newly identified rows to {output_csv}")
    else:
        print("\nNo newly identified Canada jobs found.")

    enriched = enrich_country_from_context(improved)
    enriched_country = enriched["country"].fillna("")
    enriched_canada = enriched_country.eq("CA")
    newly_enriched = enriched_canada & ~improved_canada

    print("\nContext enrichment summary:")
    print(f"- Enriched Canada count: {int(enriched_canada.sum())}")
    print(f"- Newly identified from context: {int(newly_enriched.sum())}")

    if newly_enriched.any():
        enriched_sample = enriched.loc[newly_enriched].copy()
        enriched_sample["canada_signals"] = enriched_sample.apply(_canadian_signals, axis=1)
        cols = [
            "job_key",
            "company",
            "title",
            "location_raw",
            "description_text",
            "salary_currency",
            "salary_text",
            "apply_url",
            "city",
            "region",
            "country",
            "canada_signals",
        ]
        available = [col for col in cols if col in enriched_sample.columns]
        print(f"\nSample of context-enriched Canada jobs (up to {limit}):")
        print(enriched_sample[available].head(limit).to_string(index=False))

        print("\nTop context-enriched location_raw values:")
        top_locations = (
            enriched_sample["location_raw"].fillna("").value_counts().head(limit)
        )
        for location, count in top_locations.items():
            print(f"- {location}: {count}")
    else:
        print("\nNo additional Canada jobs found via context enrichment.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare baseline vs improved Canada identification on a parquet file."
    )
    parser.add_argument("input", type=Path, help="Path to input parquet file")
    parser.add_argument(
        "--limit", type=int, default=20, help="Number of rows to display"
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional path to write newly identified rows as CSV",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, limit=args.limit, output_csv=args.output_csv)
