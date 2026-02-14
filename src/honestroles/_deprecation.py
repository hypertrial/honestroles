from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar
import warnings

P = ParamSpec("P")
R = TypeVar("R")


class HonestrolesDeprecationWarning(FutureWarning):
    """Warning category for public API deprecations in honestroles."""


def warn_deprecated(
    feature: str,
    *,
    since: str,
    remove_in: str,
    alternative: str | None = None,
    stacklevel: int = 2,
) -> None:
    """Emit a standardized deprecation warning."""
    message = f"{feature} is deprecated since honestroles {since} and will be removed in {remove_in}."
    if alternative:
        message = f"{message} Use {alternative} instead."
    warnings.warn(message, HonestrolesDeprecationWarning, stacklevel=stacklevel)


def deprecated(
    *,
    since: str,
    remove_in: str,
    alternative: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator for marking public functions as deprecated."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            warn_deprecated(
                func.__name__,
                since=since,
                remove_in=remove_in,
                alternative=alternative,
                stacklevel=3,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator
