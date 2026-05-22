"""Runnable demo: load Agree.com data into a local duckdb file.

Configure `.dlt/secrets.toml`:

    [sources.agree_com]
    api_key = "your_agree_api_key"

Then:

    python agree_com_pipeline.py
"""

from __future__ import annotations

import dlt

from agree_com import agree_com_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="agree_com_demo",
        destination="duckdb",
        dataset_name="agree_com_data",
    )
    info = pipeline.run(agree_com_source())
    print(info)


if __name__ == "__main__":
    main()
