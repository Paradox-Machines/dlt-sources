"""Runnable demo: load iNova (UKG Ready) payroll / workforce data into duckdb.

Configure `.dlt/config.toml` and `.dlt/secrets.toml`:

    [sources.inova]
    company_id = "123456"          # UKG Ready cid, or "|short_name"
    pod = "secure.saashr.com"      # tenant host — confirm with iNova

    [sources.inova]
    client_id = "..."
    client_secret = "..."

Then:

    python inova_pipeline.py
"""

from __future__ import annotations

import dlt
from inova import inova_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="inova_demo",
        destination="duckdb",
        dataset_name="inova_data",
    )
    info = pipeline.run(inova_source())
    print(info)


if __name__ == "__main__":
    main()
