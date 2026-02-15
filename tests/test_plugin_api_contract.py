from __future__ import annotations

import textwrap

import pandas as pd
import pytest

import honestroles.plugins as plugins_module
from honestroles.plugins import (
    PluginSpec,
    apply_rate_plugins,
    get_filter_plugin_spec,
    get_label_plugin_spec,
    get_rate_plugin_spec,
    list_filter_plugin_specs,
    list_label_plugin_specs,
    list_rate_plugins,
    list_rate_plugin_specs,
    load_plugins_from_module,
    register_filter_plugin,
    register_label_plugin,
    register_rate_plugin,
    unregister_rate_plugin,
)
from honestroles.rate import rate_jobs


def test_filter_plugin_spec_is_recorded_and_retrievable() -> None:
    def only_true(df: pd.DataFrame) -> pd.Series:
        return pd.Series([True] * len(df), index=df.index)

    register_filter_plugin(
        "only_true",
        only_true,
        spec=PluginSpec(api_version="1.0", plugin_version="1.2.3", capabilities=("filter",)),
    )
    spec = get_filter_plugin_spec("only_true")
    assert spec.api_version == "1.0"
    assert spec.plugin_version == "1.2.3"
    assert spec.capabilities == ("filter",)
    all_specs = list_filter_plugin_specs()
    assert all_specs["only_true"].plugin_version == "1.2.3"


def test_filter_plugin_spec_supports_mapping_with_iterable_capabilities() -> None:
    register_filter_plugin(
        "from_mapping",
        lambda df: pd.Series([True] * len(df), index=df.index),
        spec={"api_version": "1.0", "plugin_version": "0.9.0", "capabilities": ["filter", "tag"]},
    )
    spec = get_filter_plugin_spec("from_mapping")
    assert spec.capabilities == ("filter", "tag")


def test_filter_plugin_spec_invalid_format_raises() -> None:
    with pytest.raises(ValueError, match="Invalid plugin api_version"):
        register_filter_plugin(
            "bad_format",
            lambda df: pd.Series([True] * len(df), index=df.index),
            spec=PluginSpec(api_version="x"),
        )


def test_filter_plugin_spec_invalid_type_raises() -> None:
    with pytest.raises(TypeError, match="spec must be PluginSpec, mapping, or None"):
        register_filter_plugin(
            "bad_spec_type",
            lambda df: pd.Series([True] * len(df), index=df.index),
            spec=123,  # type: ignore[arg-type]
        )


def test_get_filter_plugin_spec_unknown_raises() -> None:
    with pytest.raises(KeyError, match="Unknown filter plugin"):
        get_filter_plugin_spec("missing")


def test_register_and_apply_rate_plugin() -> None:
    def add_flag(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["extra_flag"] = True
        return result

    register_rate_plugin("add_flag", add_flag)
    result = apply_rate_plugins(pd.DataFrame({"x": [1]}), ["add_flag"])
    assert result["extra_flag"].tolist() == [True]
    assert "add_flag" in list_rate_plugins()
    assert list_rate_plugin_specs()["add_flag"].capabilities == ("rate",)


def test_apply_rate_plugins_empty_list_is_noop() -> None:
    df = pd.DataFrame({"x": [1, 2]})
    result = apply_rate_plugins(df, [])
    assert result.equals(df)


def test_list_rate_plugin_specs_returns_copy() -> None:
    register_rate_plugin("copy_test", lambda df: df.assign(copy_test=True))
    specs = list_rate_plugin_specs()
    specs.pop("copy_test")
    assert "copy_test" in list_rate_plugin_specs()


def test_rate_plugin_duplicate_unless_overwrite_and_unregister() -> None:
    def v1(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["v"] = "v1"
        return result

    def v2(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["v"] = "v2"
        return result

    register_rate_plugin("versioned_rate", v1)
    with pytest.raises(ValueError, match="already registered"):
        register_rate_plugin("versioned_rate", v2)
    register_rate_plugin("versioned_rate", v2, overwrite=True)
    applied = apply_rate_plugins(pd.DataFrame({"x": [1]}), ["versioned_rate"])
    assert applied["v"].tolist() == ["v2"]
    unregister_rate_plugin("versioned_rate")
    assert "versioned_rate" not in list_rate_plugins()


def test_get_rate_and_label_specs_unknown_raises() -> None:
    with pytest.raises(KeyError, match="Unknown rate plugin"):
        get_rate_plugin_spec("missing")
    with pytest.raises(KeyError, match="Unknown label plugin"):
        get_label_plugin_spec("missing")


def test_label_plugin_spec_listing() -> None:
    register_label_plugin(
        "add_tag",
        lambda df: df.assign(tag="x"),
        spec={"api_version": "1.0", "plugin_version": "0.2.0", "capabilities": ["label"]},
    )
    specs = list_label_plugin_specs()
    assert specs["add_tag"].plugin_version == "0.2.0"


def test_get_label_and_rate_specs_success() -> None:
    register_label_plugin("label_spec_ok", lambda df: df.assign(ok=True))
    register_rate_plugin("rate_spec_ok", lambda df: df.assign(ok=True))
    assert get_label_plugin_spec("label_spec_ok").api_version == "1.0"
    assert get_rate_plugin_spec("rate_spec_ok").api_version == "1.0"


def test_apply_rate_plugins_unknown_and_wrong_type_errors() -> None:
    with pytest.raises(KeyError, match="Unknown rate plugin"):
        apply_rate_plugins(pd.DataFrame({"x": [1]}), ["missing_rate"])

    register_rate_plugin("bad_rate", lambda df: ["wrong"])  # type: ignore[return-value]
    with pytest.raises(TypeError, match="must return a pandas DataFrame"):
        apply_rate_plugins(pd.DataFrame({"x": [1]}), ["bad_rate"])


def test_rate_jobs_supports_plugin_raters(sample_df: pd.DataFrame) -> None:
    def promote_high_ratings(df: pd.DataFrame, *, threshold: float = 0.8) -> pd.DataFrame:
        result = df.copy()
        result["top_role"] = result["rating"].fillna(0).ge(threshold)
        return result

    register_rate_plugin("top_role", promote_high_ratings)
    rated = rate_jobs(
        sample_df,
        use_llm=False,
        plugin_raters=["top_role"],
        plugin_rater_kwargs={"top_role": {"threshold": 0.6}},
    )
    assert "top_role" in rated.columns
    assert rated["top_role"].dtype == bool


def test_load_plugins_from_module_path(tmp_path) -> None:
    plugin_file = tmp_path / "sample_plugins.py"
    plugin_file.write_text(
        textwrap.dedent(
            """
            import pandas as pd
            from honestroles.plugins import PluginSpec, register_filter_plugin

            def _all_rows(df: pd.DataFrame) -> pd.Series:
                return pd.Series([True] * len(df), index=df.index)

            def register() -> None:
                register_filter_plugin(
                    "module_loaded_filter",
                    _all_rows,
                    spec=PluginSpec(api_version="1.0", plugin_version="0.0.1"),
                )
            """
        ),
        encoding="utf-8",
    )

    loaded = load_plugins_from_module(str(plugin_file))
    assert loaded["filter"] == ["module_loaded_filter"]


def test_load_plugins_from_module_prefers_register_plugins_over_register(tmp_path) -> None:
    plugin_file = tmp_path / "dual_registrar.py"
    plugin_file.write_text(
        textwrap.dedent(
            """
            import pandas as pd
            from honestroles.plugins import register_filter_plugin

            def _all_rows(df: pd.DataFrame) -> pd.Series:
                return pd.Series([True] * len(df), index=df.index)

            def register_plugins() -> None:
                register_filter_plugin("from_register_plugins", _all_rows)

            def register() -> None:
                register_filter_plugin("from_register", _all_rows)
            """
        ),
        encoding="utf-8",
    )

    loaded = load_plugins_from_module(str(plugin_file))
    assert loaded["filter"] == ["from_register_plugins"]


def test_load_plugins_from_module_can_register_all_plugin_kinds(tmp_path) -> None:
    plugin_file = tmp_path / "all_kinds.py"
    plugin_file.write_text(
        textwrap.dedent(
            """
            import pandas as pd
            from honestroles.plugins import (
                register_filter_plugin,
                register_label_plugin,
                register_rate_plugin,
            )

            def _filter(df: pd.DataFrame) -> pd.Series:
                return pd.Series([True] * len(df), index=df.index)

            def _label(df: pd.DataFrame) -> pd.DataFrame:
                return df.assign(label_tag="ok")

            def _rate(df: pd.DataFrame) -> pd.DataFrame:
                return df.assign(rate_tag=True)

            def register() -> None:
                register_filter_plugin("all_filter", _filter)
                register_label_plugin("all_label", _label)
                register_rate_plugin("all_rate", _rate)
            """
        ),
        encoding="utf-8",
    )

    loaded = load_plugins_from_module(str(plugin_file))
    assert loaded["filter"] == ["all_filter"]
    assert loaded["label"] == ["all_label"]
    assert loaded["rate"] == ["all_rate"]


def test_load_plugins_from_module_requires_registrar(tmp_path) -> None:
    plugin_file = tmp_path / "bad_plugins.py"
    plugin_file.write_text("VALUE = 1\n", encoding="utf-8")
    with pytest.raises(AttributeError, match="register"):
        load_plugins_from_module(str(plugin_file))


def test_load_plugins_from_module_dotted_import(tmp_path, monkeypatch) -> None:
    package_dir = tmp_path / "pluginpkg"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text(
        textwrap.dedent(
            """
            import pandas as pd
            from honestroles.plugins import register_filter_plugin

            def _all_true(df: pd.DataFrame) -> pd.Series:
                return pd.Series([True] * len(df), index=df.index)

            def register_plugins() -> None:
                register_filter_plugin("dotted_filter", _all_true)
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    loaded = load_plugins_from_module("pluginpkg")
    assert loaded["filter"] == ["dotted_filter"]


def test_load_plugins_from_module_rejects_blank_ref() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        load_plugins_from_module("  ")


def test_load_plugins_from_module_rejects_directory_path(tmp_path) -> None:
    plugin_dir = tmp_path / "dir_plugin"
    plugin_dir.mkdir()
    with pytest.raises(ValueError, match="must be a Python file"):
        load_plugins_from_module(str(plugin_dir))


def test_load_plugins_from_module_import_error_for_bad_file_spec(tmp_path, monkeypatch) -> None:
    plugin_file = tmp_path / "broken.py"
    plugin_file.write_text("pass\n", encoding="utf-8")
    monkeypatch.setattr(plugins_module.importlib.util, "spec_from_file_location", lambda *a, **k: None)
    with pytest.raises(ImportError, match="Cannot import plugin module"):
        load_plugins_from_module(str(plugin_file))
