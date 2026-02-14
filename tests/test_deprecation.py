from __future__ import annotations

import pytest

from honestroles.deprecation import (
    HonestrolesDeprecationWarning,
    deprecated,
    warn_deprecated,
)


def test_warn_deprecated_emits_standard_warning() -> None:
    with pytest.warns(HonestrolesDeprecationWarning) as caught:
        warn_deprecated(
            "old_api",
            since="0.2.0",
            remove_in="0.4.0",
            alternative="new_api",
        )
    assert "old_api is deprecated since honestroles 0.2.0" in str(caught[0].message)
    assert "Use new_api instead." in str(caught[0].message)


def test_deprecated_decorator_warns_and_returns_value() -> None:
    @deprecated(since="0.2.0", remove_in="0.4.0", alternative="new_fn")
    def old_fn(value: int) -> int:
        return value + 1

    with pytest.warns(HonestrolesDeprecationWarning) as caught:
        result = old_fn(2)

    assert result == 3
    assert "old_fn is deprecated since honestroles 0.2.0" in str(caught[0].message)
