from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.config import SourceAdapterSpec, InputAdapterFieldConfig
from honestroles.errors import ConfigValidationError
from honestroles.io import (
    apply_source_adapter,
    infer_source_adapter,
    render_adapter_toml_fragment,
)
from honestroles.io.adapter import (
    _coerce_adapter_config,
    _name_score,
    _parse_score,
    _type_score,
)


def test_apply_source_adapter_maps_when_canonical_missing() -> None:
    df = pl.DataFrame(
        {
            "location_raw": ["Remote"],
            "remote_flag": ["yes"],
        }
    )
    cfg = SourceAdapterSpec(
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
    cfg = SourceAdapterSpec(
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
    cfg = SourceAdapterSpec(
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
    cfg = SourceAdapterSpec(
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
    cfg = SourceAdapterSpec(
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
    cfg = SourceAdapterSpec(
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


def test_render_adapter_toml_fragment_bool_and_date_defaults() -> None:
    cfg = SourceAdapterSpec(
        enabled=True,
        fields={
            "remote": InputAdapterFieldConfig.model_validate({"from": ["remote_flag"], "cast": "bool"}),
            "posted_at": InputAdapterFieldConfig.model_validate(
                {"from": ["date_posted"], "cast": "date_string"}
            ),
        },
    )
    text = render_adapter_toml_fragment(cfg)
    assert "true_values" in text
    assert "datetime_formats" in text


def test_apply_source_adapter_int_cast_and_unresolved_branch() -> None:
    df = pl.DataFrame({"x": ["1", "bad"]})
    cfg = SourceAdapterSpec(
        enabled=True,
        fields={
            "salary_min": InputAdapterFieldConfig.model_validate({"from": ["x"], "cast": "int"}),
            "salary_max": InputAdapterFieldConfig.model_validate({"from": ["missing"], "cast": "int"}),
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["salary_min"].to_list() == [1, None]
    assert "salary_max" in diagnostics["unresolved"]
    assert diagnostics["coercion_errors"]["salary_min"] == 1


def test_adapter_internal_scoring_helpers() -> None:
    assert _name_score("location", "location") == 1.0
    assert _name_score("location", "job_location") > 0.0
    assert _name_score("location", "zzz") == 0.0
    assert _type_score("bool", pl.Boolean) == 1.0
    assert _type_score("bool", pl.Date) == 0.2
    assert _type_score("float", pl.String) == 0.6
    assert _type_score("float", pl.Date) == 0.2
    assert _type_score("date_string", pl.String) == 0.8
    assert _type_score("date_string", pl.Date) == 1.0
    assert _type_score("string", pl.Int64) == 0.6
    assert _name_score("remote", "__") == 0.0

    sample = pl.DataFrame({"x": [None, None]})
    assert _parse_score(sample, "x", "string") == 0.5


def test_coerce_adapter_config_validation_errors() -> None:
    assert _coerce_adapter_config(None).enabled is False
    cfg = SourceAdapterSpec(enabled=True)
    assert _coerce_adapter_config(cfg).enabled is True

    class _CfgLike:
        def model_dump(self, mode: str = "python"):
            return {"enabled": True, "fields": {}}

    assert _coerce_adapter_config(_CfgLike()).enabled is True
    assert _coerce_adapter_config({"enabled": True, "fields": {}}).enabled is True
    with pytest.raises(TypeError):
        _coerce_adapter_config(1)


def test_apply_source_adapter_trim_and_conflict_noop_branches() -> None:
    df = pl.DataFrame({"remote": [True], "loc_text": ["  Remote  "]})
    cfg = SourceAdapterSpec(
        enabled=True,
        fields={
            "remote": InputAdapterFieldConfig.model_validate(
                {"from": ["missing"], "cast": "bool"}
            ),
            "location": InputAdapterFieldConfig.model_validate(
                {"from": ["loc_text"], "cast": "string", "trim": False}
            ),
        },
    )
    out, diagnostics = apply_source_adapter(df, cfg)
    assert out["remote"].to_list() == [True]
    assert out["location"].to_list() == ["  Remote  "]
    assert diagnostics["conflicts"] == {}
    assert diagnostics["unresolved"] == []


def test_apply_source_adapter_records_null_like_hits() -> None:
    df = pl.DataFrame({"location_raw": ["", "n/a", "Remote"]})
    cfg = SourceAdapterSpec(
        enabled=True,
        fields={
            "location": InputAdapterFieldConfig.model_validate(
                {"from": ["location_raw"], "cast": "string"}
            )
        },
    )
    _, diagnostics = apply_source_adapter(df, cfg)
    assert diagnostics["null_like_hits"]["location"] == 2


def test_infer_source_adapter_validates_more_thresholds() -> None:
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ConfigValidationError, match="top_candidates must be >= 1"):
        infer_source_adapter(df, top_candidates=0)
    with pytest.raises(ConfigValidationError, match="min_confidence must be between 0 and 1"):
        infer_source_adapter(df, min_confidence=2.0)


def test_apply_source_adapter_conflict_zero_path() -> None:
    df = pl.DataFrame({"remote": [True], "remote_flag": ["true"]})
    cfg = SourceAdapterSpec(
        enabled=True,
        fields={
            "remote": InputAdapterFieldConfig.model_validate(
                {"from": ["remote_flag"], "cast": "bool"}
            )
        },
    )
    _, diagnostics = apply_source_adapter(df, cfg)
    assert diagnostics["conflicts"] == {}
