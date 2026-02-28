from __future__ import annotations

from dataclasses import dataclass
from html import unescape

import polars as pl

from honestroles.config.models import (
    CleanStageOptions,
    FilterStageOptions,
    LabelStageOptions,
    MatchStageOptions,
    RateStageOptions,
)
from honestroles.domain import ApplicationPlanEntry, JobDataset
from honestroles.errors import StageExecutionError
from honestroles.plugins.errors import PluginExecutionError
from honestroles.plugins.types import (
    FilterStageContext,
    LabelStageContext,
    PluginDefinition,
    RateStageContext,
    RuntimeExecutionContext,
)

@dataclass(frozen=True, slots=True)
class StageArtifacts:
    application_plan: tuple[ApplicationPlanEntry, ...] = ()


def _clean_text_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.replace_all(r"(?is)<[^>]*>", " ")
        .str.replace_all(r"\\s+", " ")
        .str.strip_chars()
    )


def clean_stage(
    dataset: JobDataset,
    options: CleanStageOptions,
    runtime: RuntimeExecutionContext,
) -> JobDataset:
    _ = runtime
    try:
        dataset.validate()
        frame = dataset.to_polars(copy=False)
        text_expr = pl.col("description_text").cast(pl.String, strict=False).str.strip_chars()
        if options.strip_html:
            html_raw = pl.col("description_html").cast(pl.String, strict=False)
            frame = frame.with_columns(
                pl.when(html_raw.is_not_null() & (html_raw.str.strip_chars() != ""))
                .then(_clean_text_expr("description_html"))
                .otherwise(text_expr)
                .alias("description_text")
            )
        else:
            frame = frame.with_columns(text_expr.alias("description_text"))

        frame = frame.with_columns(
            pl.col("title").cast(pl.String, strict=False).str.strip_chars().alias("title"),
            pl.col("company").cast(pl.String, strict=False).str.strip_chars().alias("company"),
            pl.col("location").cast(pl.String, strict=False).alias("location"),
            pl.col("apply_url").cast(pl.String, strict=False).alias("apply_url"),
            pl.col("description_text").cast(pl.String, strict=False).alias("description_text"),
        )

        if options.drop_null_titles:
            frame = frame.filter(pl.col("title").is_not_null() & (pl.col("title") != ""))
        return dataset.with_frame(frame)
    except Exception as exc:
        raise StageExecutionError("clean", str(exc)) from exc


def _apply_filter_options(df: pl.DataFrame, options: FilterStageOptions) -> pl.DataFrame:
    frame = df
    if options.remote_only:
        frame = frame.filter(pl.col("remote") == pl.lit(True))
    if options.min_salary is not None:
        salary_expr = pl.coalesce([pl.col("salary_min"), pl.col("salary_max")])
        frame = frame.filter(salary_expr >= pl.lit(options.min_salary))
    if options.required_keywords:
        text = pl.concat_str(
            [
                pl.col("title").fill_null(""),
                pl.lit(" "),
                pl.col("description_text").fill_null(""),
            ],
            separator="",
        ).str.to_lowercase()
        for keyword in options.required_keywords:
            term = keyword.strip().lower()
            if term:
                frame = frame.filter(text.str.contains(term, literal=True))
    return frame


def _run_filter_plugins(
    dataset: JobDataset,
    plugins: tuple[PluginDefinition, ...],
    runtime: RuntimeExecutionContext,
) -> JobDataset:
    dataset.validate()
    result = dataset
    for plugin in plugins:
        ctx = FilterStageContext(
            plugin_name=plugin.name,
            settings=plugin.settings,
            runtime=runtime,
        )
        try:
            candidate = plugin.func(result, ctx)
        except Exception as exc:
            raise PluginExecutionError(plugin.name, plugin.kind, str(exc)) from exc
        if not isinstance(candidate, JobDataset):
            raise PluginExecutionError(
                plugin.name,
                plugin.kind,
                f"returned invalid type '{type(candidate).__name__}', expected JobDataset",
            )
        _validate_plugin_dataset(plugin, candidate)
        result = candidate
    return result


def filter_stage(
    dataset: JobDataset,
    options: FilterStageOptions,
    runtime: RuntimeExecutionContext,
    plugins: tuple[PluginDefinition, ...] = (),
) -> JobDataset:
    try:
        dataset.validate()
        base = dataset.with_frame(_apply_filter_options(dataset.to_polars(copy=False), options))
        return _run_filter_plugins(base, plugins, runtime)
    except PluginExecutionError:
        raise
    except Exception as exc:
        raise StageExecutionError("filter", str(exc)) from exc


def label_stage(
    dataset: JobDataset,
    options: LabelStageOptions,
    runtime: RuntimeExecutionContext,
    plugins: tuple[PluginDefinition, ...] = (),
) -> JobDataset:
    _ = options
    try:
        dataset.validate()
        frame = dataset.to_polars(copy=False).with_columns(
            pl.when(pl.col("title").str.contains("(?i)intern|junior|entry"))
            .then(pl.lit("junior"))
            .when(pl.col("title").str.contains("(?i)senior|staff|principal"))
            .then(pl.lit("senior"))
            .otherwise(pl.lit("mid"))
            .alias("label_seniority"),
            pl.when(pl.col("title").str.contains("(?i)data"))
            .then(pl.lit("data"))
            .when(pl.col("title").str.contains("(?i)machine learning|ml|ai"))
            .then(pl.lit("ml"))
            .when(pl.col("title").str.contains("(?i)backend|platform|infra"))
            .then(pl.lit("backend"))
            .otherwise(pl.lit("other"))
            .alias("label_role_category"),
        )

        stack_expr = (
            pl.concat_str(
                [pl.col("title").fill_null(""), pl.lit(" "), pl.col("description_text").fill_null("")],
                separator="",
            )
            .str.to_lowercase()
            .str.extract_all(r"python|sql|aws|gcp|java|rust|typescript|docker")
            .list.unique()
            .list.sort()
        )
        result = dataset.with_frame(frame.with_columns(stack_expr.alias("label_tech_stack")))

        for plugin in plugins:
            ctx = LabelStageContext(
                plugin_name=plugin.name,
                settings=plugin.settings,
                runtime=runtime,
            )
            try:
                candidate = plugin.func(result, ctx)
            except Exception as exc:
                raise PluginExecutionError(plugin.name, plugin.kind, str(exc)) from exc
            if not isinstance(candidate, JobDataset):
                raise PluginExecutionError(
                    plugin.name,
                plugin.kind,
                f"returned invalid type '{type(candidate).__name__}', expected JobDataset",
            )
            _validate_plugin_dataset(plugin, candidate)
            result = candidate
        return result
    except PluginExecutionError:
        raise
    except Exception as exc:
        raise StageExecutionError("label", str(exc)) from exc


def _bounded(expr: pl.Expr) -> pl.Expr:
    return pl.when(expr.is_finite()).then(expr.clip(0.0, 1.0)).otherwise(pl.lit(0.0))


def rate_stage(
    dataset: JobDataset,
    options: RateStageOptions,
    runtime: RuntimeExecutionContext,
    plugins: tuple[PluginDefinition, ...] = (),
) -> JobDataset:
    try:
        dataset.validate()
        df = dataset.to_polars(copy=False)
        required = ["title", "company", "description_text", "apply_url"]
        completeness = sum(
            pl.when(pl.col(name).is_not_null() & (pl.col(name).cast(pl.String, strict=False) != ""))
            .then(1.0)
            .otherwise(0.0)
            for name in required
        ) / float(len(required))

        quality = (
            pl.col("description_text")
            .cast(pl.String, strict=False)
            .fill_null("")
            .str.len_chars()
            .cast(pl.Float64)
            / pl.lit(1500.0)
        )

        frame = df.with_columns(
            _bounded(completeness).alias("rate_completeness"),
            _bounded(quality).alias("rate_quality"),
        )

        weight_sum = options.completeness_weight + options.quality_weight
        if weight_sum <= 0:
            composite = pl.lit(0.0)
        else:
            composite = (
                pl.col("rate_completeness") * options.completeness_weight
                + pl.col("rate_quality") * options.quality_weight
            ) / weight_sum

        result = dataset.with_frame(frame.with_columns(_bounded(composite).alias("rate_composite")))

        for plugin in plugins:
            ctx = RateStageContext(
                plugin_name=plugin.name,
                settings=plugin.settings,
                runtime=runtime,
            )
            try:
                candidate = plugin.func(result, ctx)
            except Exception as exc:
                raise PluginExecutionError(plugin.name, plugin.kind, str(exc)) from exc
            if not isinstance(candidate, JobDataset):
                raise PluginExecutionError(
                    plugin.name,
                plugin.kind,
                f"returned invalid type '{type(candidate).__name__}', expected JobDataset",
            )
            _validate_plugin_dataset(plugin, candidate)
            result = candidate
        return result.with_frame(
            result.to_polars(copy=False).with_columns(
                _bounded(pl.col("rate_completeness")).alias("rate_completeness"),
                _bounded(pl.col("rate_quality")).alias("rate_quality"),
                _bounded(pl.col("rate_composite")).alias("rate_composite"),
            )
        )
    except PluginExecutionError:
        raise
    except Exception as exc:
        raise StageExecutionError("rate", str(exc)) from exc


def _normalize_text(value: str | None) -> str:
    return unescape((value or "").strip().lower())


def match_stage(
    dataset: JobDataset,
    options: MatchStageOptions,
    runtime: RuntimeExecutionContext,
) -> tuple[JobDataset, StageArtifacts]:
    _ = runtime
    try:
        dataset.validate()
        df = dataset.to_polars(copy=False)
        if "rate_composite" not in df.columns:
            frame = df.with_columns(pl.lit(0.0).alias("rate_composite"))
        else:
            frame = df
        if "label_seniority" not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias("label_seniority"))
        ranked = (
            frame.with_columns(
                pl.col("rate_composite")
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .clip(0.0, 1.0)
                .alias("fit_score")
            )
            .sort("fit_score", descending=True)
            .head(options.top_k)
            .with_row_index("fit_rank", offset=1)
        )

        plan: list[ApplicationPlanEntry] = []
        for row in ranked.select(
            "fit_rank", "title", "company", "apply_url", "fit_score", "label_seniority"
        ).iter_rows(named=True):
            effort = 15
            seniority = _normalize_text(row.get("label_seniority"))
            if seniority == "senior":
                effort = 25
            elif seniority == "junior":
                effort = 12
            plan.append(
                ApplicationPlanEntry(
                    fit_rank=int(row["fit_rank"]),
                    title=row.get("title"),
                    company=row.get("company"),
                    apply_url=row.get("apply_url"),
                    fit_score=float(row.get("fit_score") or 0.0),
                    estimated_effort_minutes=effort,
                )
            )
        return dataset.with_frame(ranked), StageArtifacts(application_plan=tuple(plan))
    except Exception as exc:
        raise StageExecutionError("match", str(exc)) from exc


def _validate_plugin_dataset(plugin: PluginDefinition, candidate: JobDataset) -> None:
    try:
        candidate.validate()
    except (TypeError, ValueError) as exc:
        raise PluginExecutionError(
            plugin.name,
            plugin.kind,
            f"returned invalid JobDataset: {exc}",
        ) from exc
