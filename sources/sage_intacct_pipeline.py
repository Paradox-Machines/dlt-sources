"""Runnable demo: load Sage Intacct GL detail and sub-ledgers into local duckdb.

Configure `.dlt/secrets.toml`:

    [sources.sage_intacct]
    company_id = "interstateac"
    user_id = "ws_user"
    client_id = "your-app-client-id"
    client_secret = "your-app-client-secret"
    # optional; omit for a top-level read:
    # entity_ids = ["Interstate"]

The Web Services user must be authorised for the client application and have
read access to the Company, General Ledger, AR, AP and Projects objects.  Then:

    python sage_intacct_pipeline.py
"""

from __future__ import annotations

import dlt
from sage_intacct import sage_intacct_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sage_intacct_demo",
        destination="duckdb",
        dataset_name="sage_intacct_data",
    )
    info = pipeline.run(sage_intacct_source())
    print(info)


if __name__ == "__main__":
    main()
