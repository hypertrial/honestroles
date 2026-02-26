from __future__ import annotations

import importlib

import pytest
from hypothesis import assume, given

from .strategies import TEXT_VALUES


@pytest.mark.fuzz
def test_fuzz_init_dir_contains_exports() -> None:
    hr = importlib.reload(importlib.import_module("honestroles"))
    exported = set(hr.__all__)
    available = set(dir(hr))
    assert exported.issubset(available)


@pytest.mark.fuzz
@given(name=TEXT_VALUES)
def test_fuzz_init_getattr_unknown_name_raises(name: str) -> None:
    hr = importlib.reload(importlib.import_module("honestroles"))
    blocked = set(hr.__all__) | {
        "clean",
        "filter",
        "io",
        "label",
        "llm",
        "match",
        "plugins",
        "rate",
        "schema",
    }
    assume(name not in blocked)
    assume(name.isidentifier())

    with pytest.raises(AttributeError):
        getattr(hr, name)
