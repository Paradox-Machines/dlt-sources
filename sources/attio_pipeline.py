"""Runnable demo: load Attio data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.attio]
    api_key = "your_attio_api_key"

Then:

    python attio_pipeline.py
"""

from __future__ import annotations

import dlt

from attio import attio_source


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
