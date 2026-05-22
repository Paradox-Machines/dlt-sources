"""Runnable demo: load Pipedrive data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.pipedrive]
    api_key = "your_pipedrive_api_token"

Then:

    python pipedrive_pipeline.py
"""

from __future__ import annotations

import dlt
from pipedrive import pipedrive_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_demo",
        destination="duckdb",
        dataset_name="pipedrive_data",
    )
    info = pipeline.run(pipedrive_source())
    print(info)


if __name__ == "__main__":
    main()
