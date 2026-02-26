from __future__ import annotations

from honestroles import HonestRolesRuntime


def main() -> None:
    runtime = HonestRolesRuntime.from_configs(
        pipeline_config_path="examples/sample_pipeline.toml",
        plugin_manifest_path="examples/sample_plugins.toml",
    )
    result = runtime.run()
    print(result.diagnostics)


if __name__ == "__main__":
    main()
