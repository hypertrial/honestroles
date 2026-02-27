from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from honestroles.eda.profile_runtime import (
    build_aliases,
    parse_quality_weight_overrides,
    render_pipeline_text,
    validate_quality_config,
)
from honestroles.errors import ConfigValidationError


def test_parse_quality_weight_overrides_errors() -> None:
    with pytest.raises(ConfigValidationError, match="FIELD=WEIGHT"):
        parse_quality_weight_overrides(["x"])
    with pytest.raises(ConfigValidationError, match="non-empty"):
        parse_quality_weight_overrides([" =1"])
    with pytest.raises(ConfigValidationError, match="invalid quality weight"):
        parse_quality_weight_overrides(["x=nope"])
    with pytest.raises(ConfigValidationError, match="must be >= 0"):
        parse_quality_weight_overrides(["x=-1"])
    with pytest.raises(ConfigValidationError, match="at least one positive"):
        parse_quality_weight_overrides(["x=0", "y=0"])


def test_validate_quality_config_wraps_validation_error() -> None:
    with pytest.raises(ConfigValidationError, match="invalid EDA quality configuration"):
        validate_quality_config(quality_profile="core_fields_weighted", field_weights={"x": -1.0})


def test_build_aliases_and_render_pipeline_text() -> None:
    raw_df = pl.DataFrame({"location_raw": ["Remote"], "remote_flag": [True]})
    aliases = build_aliases(raw_df)
    assert aliases == {"location": ("location_raw",), "remote": ("remote_flag",)}

    text = render_pipeline_text(
        input_parquet_path=Path("/tmp/jobs.parquet"),
        aliases=aliases,
        profile="core_fields_weighted",
        field_weights={"posted_at": 0.5},
    )
    assert "[input.aliases]" in text
    assert "[runtime.quality.field_weights]" in text


def test_build_aliases_skips_when_canonical_present() -> None:
    raw_df = pl.DataFrame({"location_raw": ["Remote"], "location": ["Remote"], "remote": [True]})
    assert build_aliases(raw_df) == {}
