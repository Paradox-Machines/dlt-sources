"""Runnable demo: load GitHub data into a local duckdb file.

Configure `.dlt/config.toml` and `.dlt/secrets.toml`:

    # .dlt/config.toml
    [sources.github]
    org_logins = ["your-org"]

    # .dlt/secrets.toml — choose ONE auth mode
    [sources.github]
    pat_token = "ghp_..."          # personal access token, OR
    app_id = "..."                 # GitHub App
    installation_id = "..."
    private_key = "-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----"

Then:

    python github_pipeline.py
"""

from __future__ import annotations

import dlt

from github import github_source


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="github_demo",
        destination="duckdb",
        dataset_name="github_data",
    )
    info = pipeline.run(github_source())
    print(info)


if __name__ == "__main__":
    main()
