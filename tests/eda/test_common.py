from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from honestroles.eda.common import distribution_table, jsonable, serialize_scalar


def test_serialize_scalar_for_dates() -> None:
    assert serialize_scalar(date(2026, 1, 1)) == "2026-01-01"
    assert serialize_scalar(datetime(2026, 1, 1, 12, 0, 0)) == "2026-01-01T12:00:00"
    assert serialize_scalar(1) == 1


def test_jsonable_converts_tuple_path_and_dates() -> None:
    payload = {
        "path": Path("/tmp/x"),
        "nested": (date(2026, 1, 1), datetime(2026, 1, 2, 0, 0, 0)),
    }
    out = jsonable(payload)
    assert out["path"] == "/tmp/x"
    assert out["nested"] == ["2026-01-01", "2026-01-02T00:00:00"]


def test_distribution_table_empty_and_non_empty() -> None:
    empty = distribution_table([], value_column="source")
    assert empty.columns == ["source", "len", "pct"]
    assert empty.height == 0

    rows = [{"source": "lever", "len": 2, "pct": 50.0}]
    non_empty = distribution_table(rows, value_column="source")
    assert non_empty.height == 1
