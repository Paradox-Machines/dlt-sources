# AGENTS.md — paradoxmachines-dlt-sources

## Overview
Open-source library of [dlt](https://dlthub.com) verified connector sources maintained by Paradox Machines. This is the **source of truth for dlt connectors** — published to PyPI as `paradoxmachines-dlt-sources` (import package `paradox_dlt_sources`) and also consumable via `dlt init --location`. Each source (agree_com, attio, github, hubspot, loxo, notion, pipedrive, quickbooks, stripe) is a self-contained folder that extracts data from a third-party API into any dlt destination.

It follows [dlt-hub/verified-sources](https://github.com/dlt-hub/verified-sources) structural conventions. License: Apache-2.0.

## Tech Stack
- **Python** >=3.11 (test matrix: 3.11, 3.12; all other CI jobs run on 3.12)
- **dlt** >=1.0,<2 (core framework), `requests>=2.31`, `PyJWT>=2.8` (GitHub App RS256 JWT), `cryptography>=41`
- **Build**: hatchling (`pyproject.toml`, no Poetry/uv lockfile)
- **Dev/test**: pytest, pytest-cov, `responses` (HTTP mocking), `duckdb` (test destination), ruff, mypy, type stubs

## Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```
That installs the package editable plus the `dev` extras (pytest, pytest-cov, responses, ruff, mypy, duckdb, types-requests, types-PyJWT).

**Secrets for local dev:** the test suite mocks all HTTP with `responses`, so **no live credentials are needed to run tests**. For actually running a pipeline against a real API, put credentials in `.dlt/secrets.toml` (gitignored), e.g.:
```toml
[sources.attio]
api_key = "your_attio_api_key"
```
Exact key names per source are in each `paradox_dlt_sources/<name>/README.md`.

## Build
```bash
python -m build          # sdist + wheel (requires `pip install build`)
twine check dist/*       # metadata sanity check (CI gate)
```
Version is the `0.0.0` placeholder in `pyproject.toml` on `main` by design; `release.yml` patches it (in-place regex sub) from the git tag at publish time. The sdist excludes `sources/`, `examples/`, and `tests/`; the wheel ships only `paradox_dlt_sources`.

## Run (local pipeline)
PyPI/editable import flow:
```python
import dlt
from paradox_dlt_sources.attio import attio_source

pipeline = dlt.pipeline(
    pipeline_name="attio_demo", destination="duckdb", dataset_name="attio_data",
)
pipeline.run(attio_source())   # credentials resolved from .dlt/secrets.toml
```
See `examples/attio_pipeline.py` for a runnable example.

`dlt init` scaffold flow (uses the generated `sources/` mirror):
```bash
dlt init attio duckdb --location https://github.com/Paradox-Machines/dlt-sources
dlt init --list-sources --location https://github.com/Paradox-Machines/dlt-sources  # enumerate
```

## Test
```bash
pytest tests/ -v                                          # full suite
pytest tests/ --cov=paradox_dlt_sources --cov-report=term-missing  # with coverage
```
Run a **single** test directory / file / test:
```bash
pytest tests/attio/ -v
pytest tests/attio/test_attio_helpers.py -v
pytest tests/attio/test_attio_source.py::test_name -v
```
Test layout: `tests/<source>/test_<source>_helpers.py` (paginators/transforms) + `test_<source>_source.py` (full source → duckdb pipeline). **Naming exceptions:** `tests/quickbooks/` uses `test_helpers.py` / `test_source.py` (unprefixed), and `tests/github/` adds `test_github_auth.py` (dual App-JWT/PAT auth). Fixtures: canned JSON under `tests/<source>/fixtures/`, loaded via `tests/_helpers/fixture_loader.py`. Shared fixture `tmp_pipeline` (in `tests/conftest.py`) builds a throwaway duckdb pipeline with `dev_mode=True` and a `secrets.token_hex(4)` suffix so tests parallelize safely.

**Test rules:**
- Use `responses` for HTTP mocking — **not** pytest-httpx. (Stated in CONTRIBUTING.md.)
- Always pass `dev_mode=True` to `dlt.pipeline()` in tests; tests must pass against duckdb. (Stated in CONTRIBUTING.md.)
- For incremental/`since=`/early-termination patterns use `cursor.start_value` — **not** `cursor.last_value` (`last_value` shifts mid-extract → silent data loss). This convention is documented inline in source code and per-source READMEs (e.g. `paradox_dlt_sources/github/README.md`, and `github`/`notion`/`quickbooks`/`pipedrive` `__init__.py`), not in CONTRIBUTING.md.

## Lint / Typecheck / Format
```bash
ruff check .              # lint (E,F,I,B,UP,PL,SIM; line-length 100; ignores PLR0913)
ruff format --check .     # format check (drop --check to apply)
mypy paradox_dlt_sources/ # strict mode — must pass
```

## Branching & PRs
- Integration branch is **`main`** (`origin/HEAD -> origin/main`). Branch from latest `origin/main`; open PRs **against `main`**.
- Branch naming seen in history: `samsweet/par-<n>-<slug>`, `fix/<slug>`, `feat/<slug>`.
- Commits use conventional-commit prefixes (`feat(github):`, `fix(...)`, `chore:`, `docs+ci:`, `test:`, `style:`, `build:`).
- Reference the Linear ticket (PAR-###) in the PR/commit where applicable.

## CI/CD
`.github/workflows/ci.yml` runs on push + PR to `main`. Separate jobs (lint/sources-shim-sync/typecheck/coverage/package run on Python 3.12; the test job uses the 3.11/3.12 matrix):
1. **lint** — `ruff check .` and `ruff format --check .`
2. **sources-shim-sync** — `python scripts/sync_dlt_init_layout.py --check` (fails if `sources/` mirror drifted)
3. **typecheck** — `mypy paradox_dlt_sources/`
4. **test** — pytest on Python 3.11 and 3.12 with coverage
5. **coverage** — posts coverage comment + badge via `py-cov-action/python-coverage-comment-action@v3` (stores history on the `python-coverage-comment-action-data` branch; needs PR + contents write; no external SaaS)
6. **package** — `python -m build` + `twine check dist/*`

Run the local equivalent before pushing:
```bash
ruff check . && ruff format --check . && mypy paradox_dlt_sources/ && python scripts/sync_dlt_init_layout.py --check && pytest tests/ -v
```

## Deploy
PyPI via OIDC trusted publishing (`.github/workflows/release.yml`, `environment: release`). Maintainers cut a tag:
```bash
git tag v0.1.0 && git push origin v0.1.0   # or v0.1.0aN for a pre-release
```
The tag (`v*`) triggers release.yml: patch version from tag → lint → mypy → pytest → build → publish to PyPI (`pypa/gh-action-pypi-publish`, OIDC, no token) → create GitHub Release (`softprops/action-gh-release`). **No manual PyPI uploads.**

## Conventions & Gotchas
- **Sources are self-contained.** No imports across `paradox_dlt_sources/<name>/` folders. If two sources share a helper (e.g. `RefreshTokenAuth`), **inline a copy in each `helpers.py`** — cross-source imports break dlt self-containment. If you fix a bug in a duplicated helper, grep across all sources.
- **`sources/` is generated, not hand-edited.** It is a verbatim copy of `paradox_dlt_sources/` (real copies, not symlinks — dlt's `init` CLI hardcodes lookup to `<repo>/sources/` and rejects symlinks). `sources/` also includes per-source `<name>_pipeline.py` demo scripts. After editing any source, run `python scripts/sync_dlt_init_layout.py`; CI's `--check` fails PRs that drift.
- Each source folder: `__init__.py` (`@dlt.source` factory + `@dlt.resource`), `helpers.py` (HTTP/pagination/transforms), `settings.py` (endpoints/constants), `README.md`, `requirements.txt`.
- Column hints are declared even for zero-row/all-NULL syncs so dlt always materializes the column (see attio `columns(...)` usage).
- `--strict-markers` is on (`pyproject.toml` `addopts = "-ra --strict-markers"`); register any new pytest marker before using it.
- Google-style docstrings on every public function/class.
- `*.duckdb`, `*.duckdb.wal`, `.dlt/secrets.toml`, and `__pycache__/` are **gitignored**. Stray `state_test.duckdb` / `test_pipeline_*.duckdb` files and `examples/__pycache__/` in the working tree are **untracked test-run cruft (the `tmp_pipeline` fixture sometimes leaks) — they are NOT committed** (`git status --ignored` shows them as `!!`). Don't treat them as fixtures, and don't add them to git.

## Repo Layout
```
paradox_dlt_sources/        # CANONICAL source code (shipped to PyPI)
  <source>/                 # agree_com, attio, github, hubspot, loxo,
    __init__.py             #   notion, pipedrive, quickbooks, stripe
    helpers.py
    settings.py
    requirements.txt
    README.md
sources/                    # GENERATED mirror for `dlt init` (run sync script)
  <source>/                 #   + per-source <name>_pipeline.py demo scripts
tests/
  conftest.py               # tmp_pipeline fixture (duckdb, dev_mode, hex suffix)
  _helpers/fixture_loader.py
  <source>/                 # test_<source>_helpers.py, test_<source>_source.py, fixtures/
                            #   (quickbooks: test_helpers.py/test_source.py; github also has test_github_auth.py)
scripts/sync_dlt_init_layout.py   # mirror paradox_dlt_sources/ -> sources/ (CI --check)
examples/attio_pipeline.py
.github/workflows/ci.yml          # lint, shim-sync, typecheck, test(3.11/3.12), coverage, package
.github/workflows/release.yml     # tag v* -> PyPI OIDC publish + GitHub Release
pyproject.toml                    # deps, ruff, mypy, pytest, coverage config
CONTRIBUTING.md                   # porting checklist + responses/dev_mode test rules
CHANGELOG.md
```
