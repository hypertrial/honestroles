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
