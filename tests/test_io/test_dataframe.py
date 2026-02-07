import pandas as pd

from honestroles.io import validate_dataframe
from honestroles.schema import REQUIRED_COLUMNS


def test_validate_dataframe_passes(minimal_df: pd.DataFrame) -> None:
    validate_dataframe(minimal_df)


def test_validate_dataframe_missing_columns() -> None:
    df = pd.DataFrame({"job_key": ["1"]})
    try:
        validate_dataframe(df)
    except ValueError as exc:
        assert "Missing required columns" in str(exc)
        assert "company" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing columns")


def test_validate_dataframe_custom_required_columns() -> None:
    df = pd.DataFrame({"job_key": ["1"], "company": ["Acme"]})
    validate_dataframe(df, required_columns=["job_key", "company"])


def test_validate_dataframe_empty_dataframe() -> None:
    df = pd.DataFrame(columns=list(REQUIRED_COLUMNS))
    validate_dataframe(df)
