from __future__ import annotations

import importlib

import pytest


def test_package_init_lazy_attrs_and_dir() -> None:
    hr = importlib.reload(importlib.import_module("honestroles"))
    hr.__dict__.pop("match", None)

    # Submodule lazy path.
    match_module = hr.match
    assert match_module is not None

    # Attribute lazy path.
    rank_jobs = hr.rank_jobs
    assert callable(rank_jobs)

    exported = dir(hr)
    assert "rank_jobs" in exported
    assert "match" in exported


def test_package_init_unknown_attr_raises() -> None:
    hr = importlib.reload(importlib.import_module("honestroles"))
    with pytest.raises(AttributeError):
        _ = hr.not_a_real_attr
