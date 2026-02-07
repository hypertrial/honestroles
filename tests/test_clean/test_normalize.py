import pandas as pd
import pytest

from honestroles.clean.normalize import (
    enrich_country_from_context,
    normalize_employment_types,
    normalize_locations,
    normalize_salaries,
)


def test_normalize_locations(sample_df: pd.DataFrame) -> None:
    normalized = normalize_locations(sample_df)
    assert normalized.loc[0, "city"] == "New York"
    assert normalized.loc[0, "country"] == "US"


def test_normalize_locations_missing_column(sample_df: pd.DataFrame, caplog: pytest.LogCaptureFixture) -> None:
    df = sample_df.drop(columns=["location_raw"])
    normalized = normalize_locations(df)
    assert normalized.equals(df)
    assert any("Location column" in record.message for record in caplog.records)


def test_normalize_locations_empty_values() -> None:
    df = pd.DataFrame({"location_raw": [None, ""], "remote_flag": [False, False]})
    normalized = normalize_locations(df)
    assert normalized["city"].tolist() == [None, None]
    assert normalized["region"].tolist() == [None, None]
    assert normalized["country"].tolist() == [None, None]


@pytest.mark.parametrize(
    ("location", "expected_city"),
    [
        ("Paris, France / Remote", "Paris"),
        ("Berlin, Germany; London, UK", "Berlin"),
        ("Tokyo, Japan | Remote", "Tokyo"),
        ("New York, NY or Remote", "New York"),
    ],
)
def test_normalize_locations_multi_location_first(location: str, expected_city: str) -> None:
    df = pd.DataFrame({"location_raw": [location], "remote_flag": [False]})
    normalized = normalize_locations(df)
    assert normalized.loc[0, "city"] == expected_city


def test_normalize_locations_remote_flag_overrides_text() -> None:
    df = pd.DataFrame({"location_raw": ["New York, NY"], "remote_flag": [True]})
    normalized = normalize_locations(df)
    assert normalized.loc[0, "remote_type"] == "remote"


def test_normalize_locations_missing_remote_flag_column() -> None:
    df = pd.DataFrame({"location_raw": ["Remote, US"]})
    normalized = normalize_locations(df)
    assert normalized.loc[0, "remote_type"] == "remote"


@pytest.mark.parametrize(
    ("location", "expected_city", "expected_region", "expected_country", "expected_remote"),
    [
        ("New York, NY, USA", "New York", "New York", "US", None),
        ("San Francisco, CA", "San Francisco", "California", "US", None),
        ("London, UK", "London", None, "GB", None),
        ("Remote", None, None, None, "remote"),
        ("Berlin, Germany", "Berlin", None, "DE", None),
        ("New York, USA", "New York", None, "US", None),
        ("Toronto, Ontario, Canada", "Toronto", "Ontario", "CA", None),
        ("Remote, US", None, None, "US", "remote"),
        ("US", None, None, "US", None),
        ("California", None, "California", "US", None),
    ],
)
def test_normalize_locations_parsing(
    location: str,
    expected_city: str | None,
    expected_region: str | None,
    expected_country: str | None,
    expected_remote: str | None,
) -> None:
    df = pd.DataFrame({"location_raw": [location], "remote_flag": [False]})
    normalized = normalize_locations(df)
    assert normalized.loc[0, "city"] == expected_city
    assert normalized.loc[0, "region"] == expected_region
    assert normalized.loc[0, "country"] == expected_country
    assert normalized.loc[0, "remote_type"] == expected_remote


@pytest.mark.parametrize(
    ("location", "expected_city", "expected_region", "expected_country", "expected_remote"),
    [
        ("Toronto, ON, Canada", "Toronto", "Ontario", "CA", None),
        ("Toronto, ON", "Toronto", "Ontario", "CA", None),
        ("Toronto, CA", "Toronto", None, "CA", None),
        ("Vancouver, BC", "Vancouver", "British Columbia", "CA", None),
        ("Montreal, QC", "Montreal", "Quebec", "CA", None),
        ("Montréal, QC", "Montréal", "Quebec", "CA", None),
        ("Calgary, AB, Canada", "Calgary", "Alberta", "CA", None),
        ("Ottawa, Ontario, Canada", "Ottawa", "Ontario", "CA", None),
        ("Winnipeg, MB", "Winnipeg", "Manitoba", "CA", None),
        ("St. John's, NL", "St. John's", "Newfoundland and Labrador", "CA", None),
        ("Halifax, NS", "Halifax", "Nova Scotia", "CA", None),
        ("Remote, Canada", None, None, "CA", "remote"),
        ("Canada", None, None, "CA", None),
        ("Ontario", None, "Ontario", "CA", None),
        ("Gatineau, QC, Canada", "Gatineau", "Quebec", "CA", None),
        ("San Francisco, CA", "San Francisco", "California", "US", None),
        ("Denver, CO", "Denver", "Colorado", "US", None),
        ("Chicago, IL", "Chicago", "Illinois", "US", None),
        ("Redwood City, CA", "Redwood City", "California", "US", None),
        ("Mountain View, CA", "Mountain View", "California", "US", None),
        ("Irvine, CA", "Irvine", "California", "US", None),
        ("HQ - San Francisco, CA", "HQ - San Francisco", "California", "US", None),
        ("13800 Heacock St, Moreno Valley, CA, 92553", "13800 Heacock St, Moreno Valley, 92553", "California", "US", None),
        ("SomeUnknownTown, CA", "SomeUnknownTown", "California", "US", None),
    ],
)
def test_normalize_canadian_locations(
    location: str,
    expected_city: str | None,
    expected_region: str | None,
    expected_country: str | None,
    expected_remote: str | None,
) -> None:
    df = pd.DataFrame({"location_raw": [location], "remote_flag": [False]})
    normalized = normalize_locations(df)
    assert normalized.loc[0, "city"] == expected_city
    assert normalized.loc[0, "region"] == expected_region
    assert normalized.loc[0, "country"] == expected_country
    assert normalized.loc[0, "remote_type"] == expected_remote


def test_normalize_salaries(sample_df: pd.DataFrame) -> None:
    normalized = normalize_salaries(sample_df)
    assert normalized.loc[0, "salary_min"] == 120000.0
    assert normalized.loc[0, "salary_max"] == 150000.0


def test_normalize_salaries_missing_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["salary_text"])
    normalized = normalize_salaries(df)
    assert normalized.equals(df)


def test_normalize_salaries_empty_values() -> None:
    df = pd.DataFrame({"salary_text": [None, "", 12.0, "n/a"]})
    normalized = normalize_salaries(df)
    assert normalized["salary_min"].tolist() == [None, None, None, None]
    assert normalized["salary_max"].tolist() == [None, None, None, None]


def test_normalize_salaries_defaults_currency_interval() -> None:
    df = pd.DataFrame({"salary_text": ["$100000 - $120000"]})
    normalized = normalize_salaries(df)
    assert normalized.loc[0, "salary_currency"] == "USD"
    assert normalized.loc[0, "salary_interval"] == "year"


def test_normalize_salaries_swapped_range() -> None:
    df = pd.DataFrame({"salary_text": ["$150000 - $120000"]})
    normalized = normalize_salaries(df)
    assert normalized.loc[0, "salary_min"] == 120000.0
    assert normalized.loc[0, "salary_max"] == 150000.0


def test_normalize_employment_types(sample_df: pd.DataFrame) -> None:
    df = sample_df.copy()
    df["employment_type"] = ["Full Time", "Contract"]
    normalized = normalize_employment_types(df)
    assert normalized.loc[0, "employment_type"] == "full_time"
    assert normalized.loc[1, "employment_type"] == "contract"


def test_normalize_employment_types_missing_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["employment_type"], errors="ignore")
    normalized = normalize_employment_types(df)
    assert normalized.equals(df)


def test_normalize_employment_types_edge_values() -> None:
    df = pd.DataFrame({"employment_type": [None, float("nan"), "", 12, "Full-Time"]})
    normalized = normalize_employment_types(df)
    assert normalized["employment_type"].tolist() == [None, None, None, None, "full_time"]


@pytest.mark.parametrize(
    ("row", "expected_country", "expected_region"),
    [
        ({"description_text": "Role based in Canada."}, "CA", None),
        ({"salary_currency": "CAD"}, "CA", None),
        ({"description_text": "Postal code M5V 2T6"}, "CA", None),
        (
            {"benefits": ["RRSP match"], "description_text": "Based in Canada."},
            "CA",
            None,
        ),
        ({"country": "US", "description_text": "We serve Canada."}, "US", None),
        ({"country": "CA", "region": None, "description_text": "Ontario"}, "CA", "Ontario"),
        ({"description_text": "We serve Canada."}, None, None),
        ({"description_text": "No signal here."}, None, None),
    ],
)
def test_enrich_country_from_context(
    row: dict[str, object], expected_country: str | None, expected_region: str | None
) -> None:
    base = {"country": None, "region": None}
    base.update(row)
    df = pd.DataFrame([base])
    enriched = enrich_country_from_context(df)
    assert enriched.loc[0, "country"] == expected_country
    assert enriched.loc[0, "region"] == expected_region


def test_enrich_country_from_context_missing_country_column(sample_df: pd.DataFrame) -> None:
    df = sample_df.drop(columns=["country"], errors="ignore")
    enriched = enrich_country_from_context(df, country_column="country")
    assert enriched.equals(df)


def test_enrich_country_from_context_preserves_non_ca_country() -> None:
    df = pd.DataFrame(
        [
            {
                "country": "US",
                "region": None,
                "description_text": "Based in Canada.",
            }
        ]
    )
    enriched = enrich_country_from_context(df)
    assert enriched.loc[0, "country"] == "US"
