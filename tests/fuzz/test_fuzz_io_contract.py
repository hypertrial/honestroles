from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given

from honestroles.io.contract import (
    normalize_source_data_contract,
    validate_source_data_contract,
)
from honestroles.schema import (
    APPLY_URL,
    COMPANY,
    CONTENT_HASH,
    INGESTED_AT,
    JOB_ID,
    JOB_KEY,
    LOCATION_RAW,
    SOURCE,
    TITLE,
)

from .strategies import (
    ARRAY_LIKE_VALUES,
    MIXED_SCALARS,
    TIMESTAMP_LIKE_VALUES,
    URL_LIKE_VALUES,
    dataframe_for_columns,
)

_CONTRACT_FUZZ_COLUMNS = {
    JOB_KEY: MIXED_SCALARS,
    COMPANY: MIXED_SCALARS,
    SOURCE: MIXED_SCALARS,
    JOB_ID: MIXED_SCALARS,
    TITLE: MIXED_SCALARS,
    LOCATION_RAW: MIXED_SCALARS,
    APPLY_URL: URL_LIKE_VALUES,
    INGESTED_AT: TIMESTAMP_LIKE_VALUES,
    CONTENT_HASH: MIXED_SCALARS,
    "posted_at": TIMESTAMP_LIKE_VALUES,
    "updated_at": TIMESTAMP_LIKE_VALUES,
    "last_seen": TIMESTAMP_LIKE_VALUES,
    "skills": ARRAY_LIKE_VALUES,
    "languages": ARRAY_LIKE_VALUES,
    "benefits": ARRAY_LIKE_VALUES,
    "keywords": ARRAY_LIKE_VALUES,
    "remote_flag": MIXED_SCALARS,
    "visa_sponsorship": MIXED_SCALARS,
    "salary_currency": MIXED_SCALARS,
    "salary_interval": MIXED_SCALARS,
    "salary_min": MIXED_SCALARS,
    "salary_max": MIXED_SCALARS,
}


@pytest.mark.fuzz
@pytest.mark.filterwarnings("ignore:invalid value encountered in cast:RuntimeWarning")
@given(
    df=dataframe_for_columns(_CONTRACT_FUZZ_COLUMNS, max_rows=10)
)
def test_fuzz_normalize_source_data_contract_no_crash(df: pd.DataFrame) -> None:
    normalized = normalize_source_data_contract(df)
    assert len(normalized) == len(df)
    assert normalized.index.equals(df.index)
    for column in ("skills", "languages", "benefits", "keywords"):
        values = normalized[column].tolist()
        assert all(value is None or isinstance(value, list) for value in values)


@pytest.mark.fuzz
@pytest.mark.filterwarnings("ignore:invalid value encountered in cast:RuntimeWarning")
@given(
    df=dataframe_for_columns(_CONTRACT_FUZZ_COLUMNS, max_rows=10)
)
def test_fuzz_validate_source_data_contract_raises_only_value_error(df: pd.DataFrame) -> None:
    try:
        validate_source_data_contract(
            df,
            require_non_null=False,
            enforce_formats=True,
        )
    except ValueError:
        pass
