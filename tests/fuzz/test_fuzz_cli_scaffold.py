from __future__ import annotations

from pathlib import Path
import tempfile

import pytest
from hypothesis import given, settings

from honestroles.cli.scaffold_plugin import (
    _sanitize_distribution_name,
    _sanitize_package_name,
    scaffold_plugin,
)

from .strategies import PLUGIN_NAME_VALUES, TEXT_VALUES


@pytest.mark.fuzz
@given(value=TEXT_VALUES.filter(lambda text: any(ch.isalnum() for ch in text)))
def test_fuzz_scaffold_sanitizers_produce_safe_identifiers(value: str) -> None:
    dist = _sanitize_distribution_name(value)
    pkg = _sanitize_package_name(value)

    assert dist
    assert pkg
    assert dist == dist.lower()
    assert pkg == pkg.lower()


@pytest.mark.fuzz
@given(value=TEXT_VALUES.filter(lambda text: not any(ch.isalnum() for ch in text)))
def test_fuzz_scaffold_sanitizers_reject_non_alnum(value: str) -> None:
    with pytest.raises(ValueError):
        _sanitize_distribution_name(value)
    with pytest.raises(ValueError):
        _sanitize_package_name(value)


@pytest.mark.fuzz
@settings(max_examples=8)
@given(distribution_name=PLUGIN_NAME_VALUES, package_name=PLUGIN_NAME_VALUES)
def test_fuzz_scaffold_plugin_stays_within_output_dir(
    distribution_name: str,
    package_name: str,
) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        output_dir = Path(tempdir)
        destination = scaffold_plugin(
            distribution_name=distribution_name,
            package_name=package_name,
            output_dir=output_dir,
            force=True,
        )

        assert destination.exists()
        assert destination.is_dir()
        assert destination.resolve().is_relative_to(output_dir.resolve())
        assert (destination / "pyproject.toml").exists()
