"""Runnable demo: load Notion data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.notion]
    integration_token = "secret_..."

Then:

    python notion_pipeline.py
"""

from __future__ import annotations

import dlt
from notion import notion_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="notion_demo",
        destination="duckdb",
        dataset_name="notion_data",
    )
    info = pipeline.run(notion_source())
    print(info)


if __name__ == "__main__":
    main()
