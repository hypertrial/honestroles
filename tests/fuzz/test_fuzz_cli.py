from __future__ import annotations

import argparse

import pytest
from hypothesis import given

from honestroles.cli.main import main
from tests.fuzz.strategies import cli_tokens


@pytest.mark.fuzz
@given(argv=cli_tokens)
def test_cli_never_raises_unhandled(argv):
    try:
        code = main(argv)
    except SystemExit:
        return
    except argparse.ArgumentError:
        return
    assert isinstance(code, int)
