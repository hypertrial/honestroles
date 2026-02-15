from __future__ import annotations

import pandas as pd

from honestroles.plugins import apply_filter_plugins, apply_label_plugins, apply_rate_plugins
from honestroles_plugin_example.plugins import register_plugins


def test_template_plugin_contract() -> None:
    register_plugins()
    df = pd.DataFrame(
        {
            "remote_flag": [True, False],
            "source": ["greenhouse", "lever"],
            "rating": [0.95, 0.5],
        }
    )

    filtered = apply_filter_plugins(df, ["only_remote"])
    assert len(filtered) == 1

    labeled = apply_label_plugins(df, ["add_source_group"])
    assert "source_group" in labeled.columns

    rated = apply_rate_plugins(df, ["add_priority_rating"])
    assert rated["priority_role"].tolist() == [True, False]
