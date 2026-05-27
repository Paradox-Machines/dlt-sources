"""Runnable demo: load Loxo data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.loxo]
    agency_slug = "your-agency-slug"
    api_key = "your_loxo_api_key"

Then:

    python loxo_pipeline.py
"""

from __future__ import annotations

import dlt
from loxo import loxo_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="loxo_demo",
        destination="duckdb",
        dataset_name="loxo_data",
    )
    info = pipeline.run(loxo_source())
    print(info)


if __name__ == "__main__":
    main()
