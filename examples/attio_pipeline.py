"""Runnable demo: load Attio data into a local duckdb file.

Configure your API key in `.dlt/secrets.toml`:

    [sources.attio]
    api_key = "your_attio_api_key"

Then:

    python examples/attio_pipeline.py
"""
from __future__ import annotations

import dlt

from paradox_dlt_sources.attio import attio_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="attio_demo",
        destination="duckdb",
        dataset_name="attio_data",
    )
    info = pipeline.run(attio_source())
    print(info)


if __name__ == "__main__":
    main()
