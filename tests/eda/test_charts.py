from __future__ import annotations

from pathlib import Path

import pytest

from honestroles.eda import charts
from honestroles.errors import ConfigValidationError


def test_write_chart_figures_uses_placeholder_when_matplotlib_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(charts, "_load_matplotlib_pyplot", lambda: None)
    files = charts.write_chart_figures({}, tmp_path)
    assert "nulls_by_column" in files
    for filename in files.values():
        assert (tmp_path / filename).exists()


def test_write_chart_figures_falls_back_when_plotter_raises(tmp_path: Path) -> None:
    summary = {"quality": {"top_null_percentages": [object()]}}
    files = charts.write_chart_figures(summary, tmp_path)
    assert files["nulls_by_column"] == "nulls_by_column.png"
    assert (tmp_path / "nulls_by_column.png").exists()


def test_load_matplotlib_pyplot_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("matplotlib"):
            raise ImportError("boom")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert charts._load_matplotlib_pyplot() is None


def test_write_placeholder_png_raises_config_error_on_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "x.png"

    def fail(self: Path, _payload: bytes) -> int:
        raise OSError("nope")

    monkeypatch.setattr(Path, "write_bytes", fail)
    with pytest.raises(ConfigValidationError, match="cannot write figure"):
        charts._write_placeholder_png(path)
