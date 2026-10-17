"""Runnable demo: load Raiser's Edge NXT data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.raisers_edge]
    client_id = "..."
    client_secret = "..."
    refresh_token = "..."
    subscription_key = "..."
    source_timezone = "America/New_York"

Then:

    python raisers_edge_pipeline.py
"""

from __future__ import annotations

import dlt
from raisers_edge import raisers_edge_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="raisers_edge_demo",
        destination="duckdb",
        dataset_name="raisers_edge_data",
    )
    info = pipeline.run(raisers_edge_source())
    print(info)


if __name__ == "__main__":
    main()
