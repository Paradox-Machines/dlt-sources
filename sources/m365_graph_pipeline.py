"""Runnable demo: load Microsoft 365 / Graph NDA documents into local duckdb.

Configure `.dlt/secrets.toml`:

    [sources.m365_graph]
    tenant_id = "your-entra-tenant-id"
    client_id = "your-app-client-id"
    client_secret = "your-app-client-secret"
    site_id = "your-sharepoint-site-id"
    # mailbox defaults to ndas@point41.com; override if needed:
    # mailbox = "ndas@point41.com"

The app registration needs the `Mail.Read` and `Sites.Read.All` application
permissions with admin consent.  Then:

    python m365_graph_pipeline.py
"""

from __future__ import annotations

import dlt
from m365_graph import m365_graph_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="m365_graph_demo",
        destination="duckdb",
        dataset_name="m365_graph_data",
    )
    info = pipeline.run(m365_graph_source())
    print(info)


if __name__ == "__main__":
    main()
