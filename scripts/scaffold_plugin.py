#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

try:
    from honestroles.cli.scaffold_plugin import main
except ModuleNotFoundError as exc:
    if exc.name != "honestroles":
        raise
    repo_src = Path(__file__).resolve().parents[1] / "src"
    sys.path.insert(0, str(repo_src))
    from honestroles.cli.scaffold_plugin import main


if __name__ == "__main__":
    raise SystemExit(main())
