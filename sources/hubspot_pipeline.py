"""Runnable demo: load HubSpot data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.hubspot]
    api_key = "your_hubspot_private_app_token"

Then:

    python hubspot_pipeline.py
"""

from __future__ import annotations

import dlt
from hubspot import hubspot_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_demo",
        destination="duckdb",
        dataset_name="hubspot_data",
    )
    info = pipeline.run(hubspot_source())
    print(info)


if __name__ == "__main__":
    main()
