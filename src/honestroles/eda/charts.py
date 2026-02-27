from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping

from honestroles.errors import ConfigValidationError

_PLOT_FILES = {
    "nulls_by_column": "nulls_by_column.png",
    "completeness_by_source": "completeness_by_source.png",
    "remote_by_source": "remote_by_source.png",
    "posted_at_timeseries": "posted_at_timeseries.png",
    "top_locations": "top_locations.png",
}


def write_chart_figures(
    summary: Mapping[str, Any],
    figures_dir: Path,
) -> dict[str, str]:
    figures_dir.mkdir(parents=True, exist_ok=True)

    plt = _load_matplotlib_pyplot()
    if plt is None:
        for filename in _PLOT_FILES.values():
            _write_placeholder_png(figures_dir / filename)
        return dict(_PLOT_FILES)

    plotters: dict[str, Callable[[Any, Mapping[str, Any], Path], None]] = {
        "nulls_by_column": _plot_nulls_by_column,
        "completeness_by_source": _plot_completeness_by_source,
        "remote_by_source": _plot_remote_by_source,
        "posted_at_timeseries": _plot_posted_at_timeseries,
        "top_locations": _plot_top_locations,
    }

    for key, filename in _PLOT_FILES.items():
        output = figures_dir / filename
        try:
            plotters[key](plt, summary, output)
        except Exception:
            _write_placeholder_png(output)

    return dict(_PLOT_FILES)


def _load_matplotlib_pyplot():
    if "MPLCONFIGDIR" not in os.environ:
        mpl_dir = Path(tempfile.gettempdir()) / "honestroles_mpl"
        mpl_dir.mkdir(parents=True, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = str(mpl_dir)

    os.environ.setdefault("MPLBACKEND", "Agg")
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return None
    return plt


def _plot_nulls_by_column(plt, summary: Mapping[str, Any], output: Path) -> None:
    rows = summary.get("quality", {}).get("top_null_percentages", [])[:10]
    labels = [item.get("column", "") for item in rows]
    values = [float(item.get("null_pct", 0.0)) for item in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#FF9900")
    ax.set_ylabel("Null %")
    ax.set_title("Top Null Percentages")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _plot_completeness_by_source(plt, summary: Mapping[str, Any], output: Path) -> None:
    rows = summary.get("completeness", {}).get("by_source", [])[:10]
    labels = [str(item.get("source", "")) for item in rows]
    values = [float(item.get("posted_at_non_null_pct_raw", 0.0)) for item in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#A0A0C0")
    ax.set_ylabel("posted_at non-null %")
    ax.set_title("Completeness by Source")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _plot_remote_by_source(plt, summary: Mapping[str, Any], output: Path) -> None:
    rows = summary.get("completeness", {}).get("by_source", [])[:10]
    labels = [str(item.get("source", "")) for item in rows]
    values = [float(item.get("remote_true_pct_runtime", 0.0)) for item in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#404060")
    ax.set_ylabel("Remote true %")
    ax.set_title("Remote Share by Source")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _plot_posted_at_timeseries(plt, summary: Mapping[str, Any], output: Path) -> None:
    rows = summary.get("temporal", {}).get("monthly_counts", [])
    labels = [str(item.get("month", "")) for item in rows]
    values = [int(item.get("count", 0)) for item in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(labels, values, color="#FFB84D", linewidth=2)
    ax.set_ylabel("Rows")
    ax.set_title("Posted At Monthly Counts")
    step = max(1, len(labels) // 12)
    ax.set_xticks(range(0, len(labels), step))
    ax.set_xticklabels(labels[::step], rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _plot_top_locations(plt, summary: Mapping[str, Any], output: Path) -> None:
    rows = summary.get("distributions", {}).get("top_locations_runtime", [])[:10]
    labels = [str(item.get("location", "")) for item in rows]
    values = [int(item.get("len", 0)) for item in rows]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color="#202040")
    ax.set_ylabel("Rows")
    ax.set_title("Top Runtime Locations")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    plt.close(fig)


def _write_placeholder_png(path: Path) -> None:
    # 1x1 transparent PNG
    png_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\x0bIDATx\x9cc\x00\x01"
        b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    try:
        path.write_bytes(png_bytes)
    except OSError as exc:
        raise ConfigValidationError(f"cannot write figure '{path}': {exc}") from exc
