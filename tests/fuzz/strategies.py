from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
from hypothesis import strategies as st

_TEXT_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    " \t\n\r-_/.,:;()[]{}<>!?@#$%^&*+=|\\'\""
)

BOOL_LIKE_STRINGS = st.sampled_from(
    [
        "true",
        "false",
        "True",
        "False",
        " yes ",
        " no ",
        "1",
        "0",
        "remote",
        "on-site",
        "",
    ]
)

TEXT_VALUES = st.text(alphabet=_TEXT_ALPHABET, min_size=0, max_size=128)

MIXED_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000_000, max_value=10_000_000),
    st.floats(allow_nan=True, allow_infinity=True, width=32),
    BOOL_LIKE_STRINGS,
    TEXT_VALUES,
    st.builds(object),
)

TIMESTAMP_LIKE_VALUES = st.one_of(
    st.none(),
    st.datetimes(timezones=st.timezones()).map(lambda dt: dt.isoformat()),
    st.sampled_from(
        [
            "2025-01-01T00:00:00Z",
            "2025/01/01",
            "Jan 01 2025",
            "not-a-date",
            "",
            "   ",
        ]
    ),
    st.integers(min_value=0, max_value=2_000_000_000_000_000_000),
    st.floats(allow_nan=True, allow_infinity=True, width=64),
    TEXT_VALUES,
)

URL_LIKE_VALUES = st.one_of(
    st.none(),
    st.sampled_from(
        [
            "https://example.com/apply",
            "http://localhost:8080/path?q=1",
            "ftp://bad.example.com",
            "example.com/no-scheme",
            "",
            "   ",
            "://broken",
        ]
    ),
    TEXT_VALUES,
    st.integers(min_value=-1000, max_value=1000),
)

ARRAY_LIKE_VALUES = st.one_of(
    st.none(),
    TEXT_VALUES,
    st.lists(MIXED_SCALARS, min_size=0, max_size=6),
    st.tuples(MIXED_SCALARS, MIXED_SCALARS),
    st.sets(st.text(min_size=0, max_size=24), min_size=0, max_size=6),
    st.dictionaries(
        st.text(min_size=1, max_size=10),
        MIXED_SCALARS,
        min_size=0,
        max_size=4,
    ),
)


@st.composite
def dataframe_for_columns(
    draw,
    column_strategies: Mapping[str, st.SearchStrategy[object]],
    *,
    min_rows: int = 0,
    max_rows: int = 10,
) -> pd.DataFrame:
    row_count = draw(st.integers(min_value=min_rows, max_value=max_rows))
    index = draw(
        st.lists(
            st.integers(min_value=-1000, max_value=1000),
            min_size=row_count,
            max_size=row_count,
        )
    )
    data: dict[str, list[object]] = {}
    for column, strategy in column_strategies.items():
        data[column] = draw(st.lists(strategy, min_size=row_count, max_size=row_count))
    return pd.DataFrame(data, index=index)
