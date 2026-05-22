"""Runnable demo: load Stripe data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.stripe]
    api_key = "sk_live_..."

Then:

    python stripe_pipeline.py
"""

from __future__ import annotations

import dlt
from stripe import stripe_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="stripe_demo",
        destination="duckdb",
        dataset_name="stripe_data",
    )
    info = pipeline.run(stripe_source())
    print(info)


if __name__ == "__main__":
    main()
