"""Runnable demo: load QuickBooks data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.quickbooks]
    client_id = "..."
    client_secret = "..."
    refresh_token = "..."
    realm_id = "..."

Then:

    python quickbooks_pipeline.py
"""

from __future__ import annotations

import dlt

from quickbooks import quickbooks_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="quickbooks_demo",
        destination="duckdb",
        dataset_name="quickbooks_data",
    )
    info = pipeline.run(quickbooks_source())
    print(info)


if __name__ == "__main__":
    main()
