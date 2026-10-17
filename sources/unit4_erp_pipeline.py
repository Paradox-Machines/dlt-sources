"""Runnable demo: load Unit4 ERP companies into local duckdb.

Configure `.dlt/secrets.toml`:

    [sources.unit4_erp.credentials]
    type = "oauth_client_credentials"
    token_url = "https://<ids-host>/connect/token"
    client_id = "..."
    client_secret = "..."

and `.dlt/config.toml`:

    [sources.unit4_erp]
    base_url = "https://<tenant-and-environment-api-root>"
    company_ids = ["RM"]

Then:

    python unit4_erp_pipeline.py
"""

from __future__ import annotations

import dlt
from unit4_erp import unit4_erp_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="unit4_erp_demo",
        destination="duckdb",
        dataset_name="unit4_erp_data",
    )
    info = pipeline.run(unit4_erp_source())
    print(info)


if __name__ == "__main__":
    main()
