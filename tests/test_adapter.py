from __future__ import annotations

from pathlib import Path

import polars as pl

from honestroles.config import InputAdapterConfig, InputAdapterFieldConfig
from honestroles.errors import ConfigValidationError
from honestroles.io import (
    apply_source_adapter,
    infer_source_adapter,
    render_adapter_toml_fragment,
)


def test_apply_source_adapter_maps_when_canonical_missing() -> None:
    df = pl.DataFrame(
        {
            "location_raw": ["Remote"],
            "remote_flag": ["yes"],
        }
    )
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "location": InputAdapterFieldConfig.model_validate(
                {"from": ["location_raw"], "cast": "string"}
            ),
            "remote": InputAdapterFieldConfig.model_validate(
                {"from": ["remote_flag"], "cast": "bool"}
            ),
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["location"].to_list() == ["Remote"]
    assert out["remote"].to_list() == [True]
    assert diagnostics["applied"] == {"location": "location_raw", "remote": "remote_flag"}
    assert diagnostics["coercion_errors"] == {}


def test_apply_source_adapter_preserves_canonical_and_counts_conflicts() -> None:
    df = pl.DataFrame(
        {
            "remote": [True, False, True],
            "remote_flag": [True, True, False],
        }
    )
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "remote": InputAdapterFieldConfig.model_validate(
                {"from": ["remote_flag"], "cast": "bool"}
            )
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["remote"].to_list() == [True, False, True]
    assert diagnostics["applied"] == {}
    assert diagnostics["conflicts"]["remote"] == 2


def test_apply_source_adapter_date_parsing_and_numeric_errors() -> None:
    df = pl.DataFrame(
        {
            "date_posted": ["2026-01-10", "10/11/2026", "bad-date"],
            "salary_text": ["120000", "not-a-number", "140,000"],
        }
    )
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "posted_at": InputAdapterFieldConfig.model_validate(
                {
                    "from": ["date_posted"],
                    "cast": "date_string",
                    "datetime_formats": ["%Y-%m-%d", "%d/%m/%Y"],
                }
            ),
            "salary_min": InputAdapterFieldConfig.model_validate(
                {"from": ["salary_text"], "cast": "float"}
            ),
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["posted_at"].to_list()[0] is not None
    assert out["posted_at"].to_list()[1] is not None
    assert out["posted_at"].to_list()[2] is None
    assert out["salary_min"].to_list() == [120000.0, None, 140000.0]
    assert diagnostics["coercion_errors"]["posted_at"] == 1
    assert diagnostics["coercion_errors"]["salary_min"] == 1
    assert diagnostics["error_samples"][0]["reason"] in {
        "date_parse_failed",
        "float_parse_failed",
    }


def test_apply_source_adapter_multi_source_deterministic_order() -> None:
    df = pl.DataFrame({"loc_b": ["B"], "loc_a": ["A"]})
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "location": InputAdapterFieldConfig.model_validate(
                {"from": ["loc_b", "loc_a"], "cast": "string"}
            )
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["location"].to_list() == ["B"]
    assert diagnostics["applied"]["location"] == "loc_b"


def test_apply_source_adapter_error_samples_are_capped() -> None:
    df = pl.DataFrame(
        {
            "remote_flag": ["maybe" for _ in range(100)],
        }
    )
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "remote": InputAdapterFieldConfig.model_validate(
                {"from": ["remote_flag"], "cast": "bool"}
            )
        },
    )
    _, diagnostics = apply_source_adapter(df, cfg)
    assert diagnostics["coercion_errors"]["remote"] == 100
    assert len(diagnostics["error_samples"]) == 20


def test_infer_source_adapter_produces_deterministic_rankings(tmp_path: Path) -> None:
    path = tmp_path / "jobs.parquet"
    pl.DataFrame(
        {
            "job_location": ["Remote", "NYC"],
            "is_remote": ["true", "false"],
            "date_posted": ["2026-01-01", "2026-01-02"],
            "title_text": ["Engineer", "Analyst"],
        }
    ).write_parquet(path)

    df = pl.read_parquet(path)
    first = infer_source_adapter(df, sample_rows=100, top_candidates=2, min_confidence=0.5)
    second = infer_source_adapter(df, sample_rows=100, top_candidates=2, min_confidence=0.5)

    assert first.toml_fragment == second.toml_fragment
    assert first.field_suggestions == second.field_suggestions
    assert "[input.adapter]" in first.toml_fragment
    assert "[input.adapter.fields.remote]" in first.toml_fragment
    assert first.report["schema_version"] == "1.0"


def test_infer_source_adapter_parse_score_not_double_penalized() -> None:
    df = pl.DataFrame(
        {
            "is_remote": ["true", "false", "maybe"],
            "other_col": ["x", "y", "z"],
        }
    )
    result = infer_source_adapter(
        df,
        sample_rows=10,
        top_candidates=1,
        min_confidence=0.78,
    )
    assert "remote" in result.adapter_config.fields
    assert result.adapter_config.fields["remote"].from_ == ("is_remote",)


def test_infer_source_adapter_validates_thresholds() -> None:
    df = pl.DataFrame({"a": [1]})
    try:
        infer_source_adapter(df, sample_rows=0)
    except ConfigValidationError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected ConfigValidationError")


def test_render_adapter_toml_fragment() -> None:
    cfg = InputAdapterConfig(
        enabled=True,
        fields={
            "location": InputAdapterFieldConfig.model_validate(
                {"from": ["location_raw"], "cast": "string"}
            )
        },
    )
    text = render_adapter_toml_fragment(cfg)
    assert "[input.adapter]" in text
    assert "[input.adapter.fields.location]" in text
