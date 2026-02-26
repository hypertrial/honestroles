from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest
from hypothesis import given
from hypothesis import strategies as st

import honestroles.plugins as plugins_module
from honestroles.plugins import PluginExport, load_plugins_from_entrypoints, reset_plugins


@dataclass
class _FakeEntryPoint:
    name: str
    loaded: object | None = None
    error: Exception | None = None

    def load(self) -> object:
        if self.error is not None:
            raise self.error
        return self.loaded


def _all_rows(df: pd.DataFrame) -> pd.Series:
    return pd.Series([True] * len(df), index=df.index)


def _label(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(label_ep=True)


def _rate(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(rate_ep=True)


@pytest.mark.fuzz
@given(
    strict=st.booleans(),
    failure_mode=st.sampled_from(["invalid_object", "load_error"]),
)
def test_fuzz_load_plugins_from_entrypoints_strict_toggle(
    strict: bool,
    failure_mode: str,
) -> None:
    reset_plugins()
    failing_loaded: object | None = object() if failure_mode == "invalid_object" else None
    failing_error = RuntimeError("boom") if failure_mode == "load_error" else None

    group_map = {
        "honestroles.filter_plugins": [
            _FakeEntryPoint(name="ep_filter", loaded=_all_rows),
            _FakeEntryPoint(name="ep_bad", loaded=failing_loaded, error=failing_error),
        ],
        "honestroles.label_plugins": [
            _FakeEntryPoint(
                name="ep_label",
                loaded=PluginExport(kind="label", plugin=_label, name="ep_label"),
            )
        ],
        "honestroles.rate_plugins": [
            _FakeEntryPoint(
                name="ep_rate",
                loaded=PluginExport(kind="rate", plugin=_rate, name="ep_rate"),
            )
        ],
    }

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            plugins_module,
            "_get_entry_points_for_group",
            lambda group: list(group_map.get(group, [])),
        )

        if strict:
            with pytest.raises(RuntimeError):
                load_plugins_from_entrypoints(strict=True, overwrite=True)
        else:
            loaded = load_plugins_from_entrypoints(strict=False, overwrite=True)
            assert "ep_filter" in loaded["filter"]
            assert "ep_label" in loaded["label"]
            assert "ep_rate" in loaded["rate"]
