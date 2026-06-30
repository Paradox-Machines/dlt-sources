"""Runnable demo: load apollo_io data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.apollo_io]
    api_key = "your_apollo_io_api_key"

Then:

    python apollo_io_pipeline.py
"""

from __future__ import annotations

import dlt
from apollo_io import apollo_io_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="apollo_io_demo",
        destination="duckdb",
        dataset_name="apollo_io_data",
    )
    info = pipeline.run(apollo_io_source())
    print(info)


if __name__ == "__main__":
    main()
