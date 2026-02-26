from __future__ import annotations

from pathlib import Path

from honestroles.errors import RuntimeInitializationError, StageExecutionError


def test_stage_execution_error_string() -> None:
    err = StageExecutionError(stage="filter", detail="boom")
    assert str(err) == "stage 'filter' failed: boom"


def test_runtime_initialization_error_string() -> None:
    err = RuntimeInitializationError(
        pipeline_config_path=Path("/tmp/pipeline.toml"),
        detail="missing file",
    )
    assert (
        str(err)
        == "runtime initialization failed for '/tmp/pipeline.toml': missing file"
    )
