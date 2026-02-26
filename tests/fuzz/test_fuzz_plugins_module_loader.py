from __future__ import annotations

from pathlib import Path
import tempfile
import textwrap
from uuid import uuid4

import pytest
from hypothesis import given

from honestroles.plugins import (
    _import_plugin_module,
    list_filter_plugins,
    load_plugins_from_module,
    reset_plugins,
)

from .strategies import PLUGIN_NAME_VALUES


@pytest.mark.fuzz
@given(plugin_name=PLUGIN_NAME_VALUES)
def test_fuzz_load_plugins_from_module_path_registers_filter(plugin_name: str) -> None:
    reset_plugins()
    safe_name = "".join(ch if ch.isalnum() else "_" for ch in plugin_name)
    with tempfile.TemporaryDirectory() as tempdir:
        plugin_file = Path(tempdir) / f"fuzz_plugin_{safe_name}_{uuid4().hex}.py"
        plugin_file.write_text(
            textwrap.dedent(
                f"""
                import pandas as pd
                from honestroles.plugins import register_filter_plugin

                def _all_rows(df: pd.DataFrame) -> pd.Series:
                    return pd.Series([True] * len(df), index=df.index)

                def register() -> None:
                    register_filter_plugin({plugin_name!r}, _all_rows, overwrite=True)
                """
            ),
            encoding="utf-8",
        )

        loaded = load_plugins_from_module(str(plugin_file))
        assert set(loaded["filter"]).issubset({plugin_name})
        assert plugin_name in list_filter_plugins()


@pytest.mark.fuzz
def test_import_plugin_module_rejects_directory(tmp_path) -> None:
    plugin_dir = tmp_path / "plugins"
    plugin_dir.mkdir()
    with pytest.raises(ValueError):
        _import_plugin_module(str(plugin_dir))


@pytest.mark.fuzz
def test_load_plugins_from_module_requires_registrar(tmp_path) -> None:
    plugin_file = tmp_path / "missing_registrar.py"
    plugin_file.write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(AttributeError):
        load_plugins_from_module(str(plugin_file))
