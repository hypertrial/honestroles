from __future__ import annotations

from pathlib import Path

import pytest

from honestroles.eda.rules import load_eda_rules
from honestroles.errors import ConfigValidationError


def test_load_eda_rules_defaults() -> None:
    rules = load_eda_rules()
    assert rules.gate.fail_on == ("P0",)
    assert rules.gate.warn_on == ("P1",)
    assert rules.drift.numeric_warn_psi == 0.10


def test_load_eda_rules_from_file_and_cli_override(tmp_path: Path) -> None:
    rules_path = tmp_path / "eda-rules.toml"
    rules_path.write_text(
        """
[gate]
fail_on = ["P0", "P1"]
warn_on = ["P2"]
max_p0 = 0
max_p1 = 5

[drift]
numeric_warn_psi = 0.2
numeric_fail_psi = 0.4
categorical_warn_jsd = 0.12
categorical_fail_jsd = 0.22
columns_numeric = ["salary_min"]
columns_categorical = ["source", "remote"]
""".strip(),
        encoding="utf-8",
    )

    rules = load_eda_rules(
        rules_file=rules_path,
        fail_on="P0",
        warn_on="P1,P2",
    )
    assert rules.gate.fail_on == ("P0",)
    assert rules.gate.warn_on == ("P1", "P2")
    assert rules.gate.max_p1 == 5
    assert rules.drift.columns_numeric == ("salary_min",)


def test_load_eda_rules_invalid_severity_raises(tmp_path: Path) -> None:
    rules_path = tmp_path / "bad-rules.toml"
    rules_path.write_text(
        """
[gate]
fail_on = ["P9"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match="unsupported severity"):
        load_eda_rules(rules_file=rules_path)


def test_load_eda_rules_default_file_discovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rules_path = tmp_path / "eda-rules.toml"
    rules_path.write_text("[gate]\nmax_p1 = 7\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    rules = load_eda_rules()
    assert rules.gate.max_p1 == 7


def test_load_eda_rules_file_validation_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="does not exist"):
        load_eda_rules(rules_file=tmp_path / "missing.toml")

    directory = tmp_path / "rules_dir"
    directory.mkdir()
    with pytest.raises(ConfigValidationError, match="not a file"):
        load_eda_rules(rules_file=directory)


def test_load_eda_rules_invalid_toml_and_table_types(tmp_path: Path) -> None:
    bad_toml = tmp_path / "bad.toml"
    bad_toml.write_text("[gate\nx=1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="invalid EDA rules file"):
        load_eda_rules(rules_file=bad_toml)

    bad_gate = tmp_path / "bad_gate.toml"
    bad_gate.write_text("gate = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="\\[gate\\] must be a table"):
        load_eda_rules(rules_file=bad_gate)

    bad_drift = tmp_path / "bad_drift.toml"
    bad_drift.write_text("drift = 1", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="\\[drift\\] must be a table"):
        load_eda_rules(rules_file=bad_drift)


def test_load_eda_rules_validation_errors_for_thresholds_and_types(tmp_path: Path) -> None:
    invalid_numeric_order = tmp_path / "numeric_order.toml"
    invalid_numeric_order.write_text(
        """
[drift]
numeric_warn_psi = 0.3
numeric_fail_psi = 0.2
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="numeric_fail_psi must be >="):
        load_eda_rules(rules_file=invalid_numeric_order)

    invalid_categorical_order = tmp_path / "cat_order.toml"
    invalid_categorical_order.write_text(
        """
[drift]
categorical_warn_jsd = 0.3
categorical_fail_jsd = 0.2
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="categorical_fail_jsd must be >="):
        load_eda_rules(rules_file=invalid_categorical_order)

    invalid_types = tmp_path / "invalid_types.toml"
    invalid_types.write_text(
        """
[gate]
max_p0 = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="must be an integer"):
        load_eda_rules(rules_file=invalid_types)


def test_load_eda_rules_list_parsing_validation(tmp_path: Path) -> None:
    invalid_csv = tmp_path / "invalid_csv.toml"
    invalid_csv.write_text("[gate]\nfail_on = []\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must include at least one severity"):
        load_eda_rules(rules_file=invalid_csv, fail_on=",")

    invalid_column_entries = tmp_path / "invalid_cols.toml"
    invalid_column_entries.write_text(
        """
[drift]
columns_categorical = ["source", ""]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="non-empty"):
        load_eda_rules(rules_file=invalid_column_entries)

    invalid_column_type = tmp_path / "invalid_col_type.toml"
    invalid_column_type.write_text(
        """
[drift]
columns_numeric = [1]
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="entries must be strings"):
        load_eda_rules(rules_file=invalid_column_type)


def test_load_eda_rules_negative_gate_thresholds_rejected(tmp_path: Path) -> None:
    rules_path = tmp_path / "neg_gate.toml"
    rules_path.write_text(
        """
[gate]
max_p0 = -1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="max_p0"):
        load_eda_rules(rules_file=rules_path)

    rules_path.write_text(
        """
[gate]
max_p1 = -1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="max_p1"):
        load_eda_rules(rules_file=rules_path)


def test_load_eda_rules_drift_negative_and_type_errors(tmp_path: Path) -> None:
    rules_path = tmp_path / "neg_drift.toml"
    rules_path.write_text(
        """
[drift]
numeric_warn_psi = -0.1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="numeric thresholds"):
        load_eda_rules(rules_file=rules_path)

    rules_path.write_text(
        """
[drift]
categorical_warn_jsd = -0.1
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="categorical thresholds"):
        load_eda_rules(rules_file=rules_path)

    rules_path.write_text(
        """
[drift]
numeric_warn_psi = "x"
""".strip(),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="must be a number"):
        load_eda_rules(rules_file=rules_path)


def test_load_eda_rules_parsers_cover_scalar_and_duplicate_paths(tmp_path: Path) -> None:
    scalar_severity = tmp_path / "scalar.toml"
    scalar_severity.write_text("[gate]\nfail_on = \"P0\"\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must be an array"):
        load_eda_rules(rules_file=scalar_severity)

    non_string_severity = tmp_path / "non_string.toml"
    non_string_severity.write_text("[gate]\nfail_on = [1]\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="entries must be strings"):
        load_eda_rules(rules_file=non_string_severity)

    scalar_columns = tmp_path / "scalar_cols.toml"
    scalar_columns.write_text("[drift]\ncolumns_numeric = \"salary_min\"\n", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must be an array"):
        load_eda_rules(rules_file=scalar_columns)

    duplicate_items = tmp_path / "dupes.toml"
    duplicate_items.write_text(
        """
[gate]
fail_on = ["P0", "P0"]

[drift]
columns_categorical = ["source", "source", "remote"]
""".strip(),
        encoding="utf-8",
    )
    rules = load_eda_rules(rules_file=duplicate_items)
    assert rules.gate.fail_on == ("P0",)
    assert rules.drift.columns_categorical == ("source", "remote")
