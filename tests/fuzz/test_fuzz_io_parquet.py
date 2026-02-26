from __future__ import annotations

from pathlib import Path
import tempfile

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

from honestroles.io import iter_parquet_row_groups, read_parquet, write_parquet

from .strategies import column_subsets, parquet_dataframe


@pytest.mark.fuzz
@given(
    df=parquet_dataframe(max_rows=18),
    data=st.data(),
)
def test_fuzz_parquet_roundtrip_and_row_groups(
    df: pd.DataFrame,
    data: st.DataObject,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        path = Path(tempdir) / "fuzz.parquet"
        write_parquet(df, path)

        loaded = read_parquet(path, validate=False)
        assert len(loaded) == len(df)

        subset = data.draw(column_subsets(list(df.columns)))
        chunks = list(iter_parquet_row_groups(path, columns=subset or None, validate=False))
        assert chunks
        assert sum(len(chunk) for chunk in chunks) == len(df)
        if subset:
            for chunk in chunks:
                assert list(chunk.columns) == subset
