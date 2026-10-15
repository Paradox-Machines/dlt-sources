"""Runnable demo: load ServiceTitan tenants into local duckdb.

Configure `.dlt/secrets.toml`:

    [sources.servicetitan]
    app_key = "ak1.your-app-key"

    [[sources.servicetitan.tenants]]
    tenant_id = "1234567"
    name = "Interstate"
    client_id = "cid.interstate"
    client_secret = "cs1.interstate"

Add one `[[sources.servicetitan.tenants]]` block per tenant.  Then:

    python servicetitan_pipeline.py
"""

from __future__ import annotations

import dlt
from servicetitan import servicetitan_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="servicetitan_demo",
        destination="duckdb",
        dataset_name="servicetitan_data",
    )
    info = pipeline.run(servicetitan_source())
    print(info)


if __name__ == "__main__":
    main()
