from __future__ import annotations

import re

import pytest
from hypothesis import given

from honestroles.io.duckdb_io import _validate_read_query, _validate_table_name

from .strategies import TEXT_VALUES

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@pytest.mark.fuzz
@given(query=TEXT_VALUES)
def test_fuzz_validate_read_query_accept_reject_contract(query: str) -> None:
    try:
        normalized = _validate_read_query(query)
    except ValueError:
        return

    assert normalized == normalized.strip()
    assert normalized
    assert ";" not in normalized
    assert re.match(r"^(select|with)\b", normalized, re.IGNORECASE) is not None


@pytest.mark.fuzz
@given(name=TEXT_VALUES)
def test_fuzz_validate_table_name_accept_reject_contract(name: str) -> None:
    try:
        normalized = _validate_table_name(name)
    except ValueError:
        return

    assert normalized == name.strip()
    assert _TABLE_RE.fullmatch(normalized) is not None

