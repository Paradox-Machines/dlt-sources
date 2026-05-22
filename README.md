# paradoxmachines-dlt-sources

[![PyPI](https://img.shields.io/pypi/v/paradoxmachines-dlt-sources.svg)](https://pypi.org/project/paradoxmachines-dlt-sources/)
[![Python versions](https://img.shields.io/pypi/pyversions/paradoxmachines-dlt-sources.svg)](https://pypi.org/project/paradoxmachines-dlt-sources/)
[![CI](https://github.com/Paradox-Machines/dlt-sources/actions/workflows/ci.yml/badge.svg)](https://github.com/Paradox-Machines/dlt-sources/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/Paradox-Machines/dlt-sources/python-coverage-comment-action-data/badge.svg)](https://github.com/Paradox-Machines/dlt-sources/tree/python-coverage-comment-action-data)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy: strict](https://img.shields.io/badge/mypy-strict-blue.svg)](https://mypy-lang.org/)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)

Open-source [dlt](https://dlthub.com) verified sources maintained by
[Paradox Machines](https://paradoxmachines.com).

This repo follows the structural conventions of
[dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources)
— each source is a self-contained folder under `paradox_dlt_sources/`
with its own README, helpers, settings, and requirements.

## Install

Two supported flows:

### A. As a PyPI dependency (recommended for production)

```bash
pip install paradoxmachines-dlt-sources
```

Pinnable, reproducible, no scaffolding.

### B. Via `dlt init` (scaffold a starter project)

Each source is also mirrored at `sources/<name>/` so dlt's CLI can scaffold
a standalone project the same way it does for
[dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources):

```bash
dlt init attio duckdb --location https://github.com/Paradox-Machines/dlt-sources
```

This drops `attio/`, `attio_pipeline.py`, `requirements.txt`, and
`.dlt/{secrets,config}.toml` into your cwd, with secret keys pre-populated
from the source signature. Substitute any of `agree_com`, `attio`, `github`,
`hubspot`, `notion`, `pipedrive`, `quickbooks`, `stripe` for the source name,
and any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations)
for `duckdb`. Run `dlt init --list-sources --location <url>` to enumerate.

> **Maintainers:** the `sources/` tree is generated. After editing any
> `paradox_dlt_sources/<name>/`, run `python scripts/sync_dlt_init_layout.py`
> to mirror the changes. CI runs `--check` to fail PRs that drift.

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

## Usage (PyPI flow)

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

For the `dlt init` flow, the scaffolded `<source>_pipeline.py` is the
equivalent demo — import paths are bare (`from attio import attio_source`)
because the source folder lands at your project root.

## License

Apache 2.0 — see [LICENSE](LICENSE).
