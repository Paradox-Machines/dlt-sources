# Contributing to paradoxmachines-dlt-sources

Thanks for your interest. This repo follows
[dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources)
structural conventions so anyone familiar with that project can
contribute here.

## Quickstart

```bash
git clone https://github.com/paradox-machines/dlt-sources.git
cd dlt-sources
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
ruff check . && ruff format --check . && mypy paradox_dlt_sources/
```

## Adding a new source

Each source lives in `paradox_dlt_sources/<name>/` and is **self-contained**
— no imports across source folders. Layout:

```
paradox_dlt_sources/<name>/
├── __init__.py        # @dlt.source factory + @dlt.resource definitions
├── helpers.py         # HTTP, pagination, transforms
├── settings.py        # constants (endpoints, whitelists)
├── README.md          # docs (use existing sources as a template)
└── requirements.txt   # dlt + source-specific deps
```

Add tests under `tests/<name>/` with at minimum:
- `test_<name>_helpers.py` — unit tests for paginators/transforms
- `test_<name>_source.py` — full source → duckdb pipeline assertions
- `fixtures/` — canned JSON HTTP responses

## Shared code

If two or more sources share a helper (e.g. `RefreshTokenAuth`), inline a
copy in each source's `helpers.py`. The cost of duplication is small;
cross-source imports break dlt's self-containment rule. **If you fix a bug
in a shared helper, grep across all sources.**

## Tests

- Use [`responses`](https://pypi.org/project/responses/) for HTTP mocking
  (not pytest-httpx).
- Always use `dev_mode=True` when constructing `dlt.pipeline()` in tests.
- Add a random suffix to pipeline names so tests can run in parallel.
- Tests must pass against duckdb. Postgres matrix is planned for v0.2.

## Style

- `ruff check . && ruff format .` before pushing.
- `mypy paradox_dlt_sources/` must pass (strict mode).
- Google-style docstrings on every public function/class.

## Releases

Maintainers cut a tag (`vX.Y.Z` or `vX.Y.ZaN` for pre-releases) which
triggers `release.yml` to publish to PyPI via OIDC. No manual PyPI uploads.
