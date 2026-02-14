import pandas as pd
import pytest

from honestroles.io import (
    normalize_source_data_contract,
    validate_source_data_contract,
)


def test_validate_source_data_contract_passes(minimal_df: pd.DataFrame) -> None:
    validate_source_data_contract(minimal_df)


def test_validate_source_data_contract_allows_extra_columns(minimal_df: pd.DataFrame) -> None:
    df = minimal_df.copy()
    df["extra_col"] = ["ok"]
    validate_source_data_contract(df)


def test_validate_source_data_contract_fails_on_null_required() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": None,
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "hash1",
            }
        ]
    )
    with pytest.raises(ValueError, match="null values"):
        validate_source_data_contract(df)


def test_validate_source_data_contract_non_null_check_can_be_disabled() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": None,
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-01T00:00:00Z",
                "content_hash": "hash1",
            }
        ]
    )
    validate_source_data_contract(df, require_non_null=False)


def test_validate_source_data_contract_missing_columns_message_is_sorted() -> None:
    df = pd.DataFrame({"alpha": ["1"]})
    with pytest.raises(ValueError) as exc:
        validate_source_data_contract(df, required_columns=["zeta", "alpha", "beta"])
    assert str(exc.value) == "Missing required columns: beta, zeta"


def test_validate_source_data_contract_null_message_is_sorted_and_stable() -> None:
    df = pd.DataFrame({"id": [1], "text": [pd.NA], "metric": [float("nan")]})
    with pytest.raises(ValueError) as exc:
        validate_source_data_contract(df, required_columns=["metric", "text", "id"])
    assert str(exc.value) == (
        "Required columns contain null values: metric (1 null), text (1 null)"
    )


def test_validate_source_data_contract_custom_required_columns() -> None:
    df = pd.DataFrame({"id": ["1"], "payload": ["ok"], "other": ["x"]})
    validate_source_data_contract(df, required_columns=["id", "payload"])


def test_validate_source_data_contract_custom_required_columns_non_null_enforced() -> None:
    df = pd.DataFrame({"id": ["1"], "payload": [None], "other": ["x"]})
    with pytest.raises(ValueError, match="payload"):
        validate_source_data_contract(df, required_columns=["id", "payload"])


def test_validate_source_data_contract_custom_required_columns_non_null_disabled() -> None:
    df = pd.DataFrame({"id": ["1"], "payload": [None], "other": ["x"]})
    validate_source_data_contract(
        df,
        required_columns=["id", "payload"],
        require_non_null=False,
    )


def test_validate_source_data_contract_empty_dataframe_short_circuits_non_null_check() -> None:
    df = pd.DataFrame({"id": [], "payload": []})
    validated = validate_source_data_contract(df, required_columns=["id", "payload"])
    assert validated.empty


def test_normalize_source_data_contract_timestamps_and_arrays() -> None:
    df = pd.DataFrame(
        [
            {
                "ingested_at": "2025-01-03 10:00:00",
                "posted_at": "2025/01/01",
                "skills": '["Python", "SQL"]',
                "languages": "English, French",
                "benefits": "401k;healthcare",
                "keywords": "backend",
            }
        ]
    )

    normalized = normalize_source_data_contract(df)

    assert normalized.loc[0, "ingested_at"].endswith("Z")
    assert normalized.loc[0, "posted_at"].endswith("Z")
    assert normalized.loc[0, "skills"] == ["Python", "SQL"]
    assert normalized.loc[0, "languages"] == ["English", "French"]
    assert normalized.loc[0, "benefits"] == ["401k", "healthcare"]
    assert normalized.loc[0, "keywords"] == ["backend"]


def test_normalize_source_data_contract_can_enable_validation() -> None:
    df = pd.DataFrame(
        [
            {
                "job_key": "acme::greenhouse::1",
                "company": "Acme",
                "source": "greenhouse",
                "job_id": "1",
                "title": "Engineer",
                "location_raw": "Remote",
                "apply_url": "https://example.com/apply",
                "ingested_at": "2025-01-01 08:00:00",
                "content_hash": "hash1",
                "skills": "Python,SQL",
            }
        ]
    )

    normalized = normalize_source_data_contract(df)
    validate_source_data_contract(normalized)
    assert normalized.loc[0, "skills"] == ["Python", "SQL"]


def test_normalize_source_data_contract_timestamp_variants() -> None:
    naive = pd.Timestamp("2025-01-03 10:00:00")
    aware = pd.Timestamp("2025-01-03 10:00:00", tz="US/Eastern")
    sentinel = object()
    df = pd.DataFrame(
        {
            "ts": [
                None,
                float("nan"),
                naive,
                aware,
                "   ",
                "not-a-date",
                1735862400000000000,
                sentinel,
            ]
        }
    )

    normalized = normalize_source_data_contract(
        df, timestamp_columns=["ts"], array_columns=[]
    )
    values = normalized["ts"].tolist()
    assert values[0] is None
    assert values[1] is None
    assert values[2] == "2025-01-03T10:00:00Z"
    assert values[3] == "2025-01-03T15:00:00Z"
    assert values[4] is None
    assert values[5] == "not-a-date"
    assert values[6] == "2025-01-03T00:00:00Z"
    assert values[7] is sentinel


def test_normalize_source_data_contract_array_variants() -> None:
    df = pd.DataFrame(
        {
            "arr": [
                None,
                float("nan"),
                "   ",
                '["Python", " ", "SQL"]',
                '[" ", ""]',
                "[not-json]",
                ",,,",
                "alpha,beta",
                "alpha;beta",
                "single",
                ("AWS", " "),
                {"Docker", " "},
                42,
            ]
        }
    )

    normalized = normalize_source_data_contract(df, timestamp_columns=[], array_columns=["arr"])
    values = normalized["arr"].tolist()
    assert values[0] is None
    assert values[1] is None
    assert values[2] is None
    assert values[3] == ["Python", "SQL"]
    assert values[4] is None
    assert values[5] == ["[not-json]"]
    assert values[6] is None
    assert values[7] == ["alpha", "beta"]
    assert values[8] == ["alpha", "beta"]
    assert values[9] == ["single"]
    assert values[10] == ["AWS"]
    assert values[11] == ["Docker"]
    assert values[12] == ["42"]
