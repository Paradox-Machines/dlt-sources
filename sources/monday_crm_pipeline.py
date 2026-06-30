"""Runnable demo: load monday_crm data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.monday_crm]
    api_key = "your_monday_crm_api_key"

Then:

    python monday_crm_pipeline.py
"""

from __future__ import annotations

import dlt
from monday_crm import monday_crm_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="monday_crm_demo",
        destination="duckdb",
        dataset_name="monday_crm_data",
    )
    info = pipeline.run(monday_crm_source())
    print(info)


if __name__ == "__main__":
    main()
