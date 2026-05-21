# paradoxmachines-dlt-sources

Open-source [dlt](https://dlthub.com) verified sources maintained by
[Paradox Machines](https://paradoxmachines.com).

This repo follows the structural conventions of
[dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources)
— each source is a self-contained folder under `paradox_dlt_sources/`
with its own README, helpers, settings, and requirements.

## Install

```bash
pip install paradoxmachines-dlt-sources
```

## Sources

| Source | Status | Resources |
|---|---|---|
| [agree_com](paradox_dlt_sources/agree_com/README.md) | beta | agreements, contacts, invoices, schedules |
| [attio](paradox_dlt_sources/attio/README.md) | beta | companies, people, deals, lists, notes |
| [github](paradox_dlt_sources/github/README.md) | beta | organizations, users, repositories, pull_requests, commits, pull_request_commits, pull_request_stats |
| [hubspot](paradox_dlt_sources/hubspot/README.md) | beta | companies, contacts, deals, engagements, deal_pipelines |
| [notion](paradox_dlt_sources/notion/README.md) | beta | users, databases, pages, blocks, comments |
| [pipedrive](paradox_dlt_sources/pipedrive/README.md) | beta | persons, deals, leads, activities, organizations, stages, users_recents |
| [quickbooks](paradox_dlt_sources/quickbooks/README.md) | beta | 24 entities (invoices, customers, accounts, transactions, …) |
| [stripe](paradox_dlt_sources/stripe/README.md) | beta | charges, customers, invoices, refunds |

More sources coming as we port them from our internal pipeline. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the porting checklist.

## Usage

```python
import dlt
from paradox_dlt_sources.attio import attio_source

pipeline = dlt.pipeline(
    pipeline_name="attio_demo",
    destination="duckdb",
    dataset_name="attio_data",
)
pipeline.run(attio_source())  # api_key resolved from .dlt/secrets.toml
```

Configure `.dlt/secrets.toml`:

```toml
[sources.attio]
api_key = "your_attio_api_key"
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
