from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest

from honestroles.eda import charts
from honestroles.errors import ConfigValidationError


class _FakeAxes:
    def bar(self, *_args, **_kwargs) -> None:
        return None

    def set_ylabel(self, *_args, **_kwargs) -> None:
        return None

    def set_title(self, *_args, **_kwargs) -> None:
        return None

    def tick_params(self, *_args, **_kwargs) -> None:
        return None

    def plot(self, *_args, **_kwargs) -> None:
        return None

    def set_xticks(self, *_args, **_kwargs) -> None:
        return None

    def set_xticklabels(self, *_args, **_kwargs) -> None:
        return None


class _FakeFigure:
    def tight_layout(self) -> None:
        return None

    def savefig(self, output: Path, **_kwargs) -> None:
        output.write_bytes(b"fake")


class _FakePyplot:
    def subplots(self, **_kwargs):
        return _FakeFigure(), _FakeAxes()

    def close(self, *_args, **_kwargs) -> None:
        return None


def test_write_chart_figures_uses_placeholder_when_matplotlib_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(charts, "_load_matplotlib_pyplot", lambda: None)
    files = charts.write_chart_figures({}, tmp_path)
    assert "nulls_by_column" in files
    for filename in files.values():
        assert (tmp_path / filename).exists()


def test_write_chart_figures_falls_back_when_plotter_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(charts, "_load_matplotlib_pyplot", lambda: _FakePyplot())
    summary = {"quality": {"top_null_percentages": [object()]}}
    files = charts.write_chart_figures(summary, tmp_path)
    assert files["nulls_by_column"] == "nulls_by_column.png"
    for filename in files.values():
        assert (tmp_path / filename).exists()


def test_load_matplotlib_pyplot_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("matplotlib"):
            raise ImportError("boom")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert charts._load_matplotlib_pyplot() is None


def test_load_matplotlib_pyplot_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MPLCONFIGDIR", raising=False)
    monkeypatch.delenv("MPLBACKEND", raising=False)

    fake_matplotlib = types.ModuleType("matplotlib")
    setattr(fake_matplotlib, "use", lambda *_args, **_kwargs: None)
    fake_pyplot = types.ModuleType("matplotlib.pyplot")

    monkeypatch.setitem(sys.modules, "matplotlib", fake_matplotlib)
    monkeypatch.setitem(sys.modules, "matplotlib.pyplot", fake_pyplot)

    assert charts._load_matplotlib_pyplot() is fake_pyplot
    assert os.environ["MPLBACKEND"] == "Agg"
    assert os.environ["MPLCONFIGDIR"]


def test_write_placeholder_png_raises_config_error_on_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "x.png"

    def fail(self: Path, _payload: bytes) -> int:
        raise OSError("nope")

    monkeypatch.setattr(Path, "write_bytes", fail)
    with pytest.raises(ConfigValidationError, match="cannot write figure"):
        charts._write_placeholder_png(path)
