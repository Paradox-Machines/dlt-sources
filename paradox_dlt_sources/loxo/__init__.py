"""Loxo dlt source — people, jobs, companies, deals, activities, users.

Spec: https://loxo.readme.io/reference/loxo-api

Auth: Bearer API token. Base URL is per-agency —
`https://{domain}/api/{agency_slug}/...` — but `domain` is `app.loxo.co`
for nearly every customer, so only two values are per-customer secrets:
`agency_slug` and `api_key`. Override `domain` via the `LOXO_API_BASE_URL`
env var for the rare custom-domain agency.

Pagination is mixed:
- `scroll_id` (cursor) — people, companies, deals, activities. Pass the
  `scroll_id` from the response back as a query param on the next call.
- `page` / `per_page` — jobs only. Stock `PageNumberPaginator`.

# OPEN QUESTIONS — settle these with a sandbox key before locking design

1. **No documented `updated_at` filter.** Listing endpoints don't appear
   to support a `since=` / `updated_at_from=` parameter, so every resource
   here is `write_disposition="replace"` for now. If a filter DOES exist
   in practice, switch to `append` + `dlt.sources.incremental("updated_at",
   initial_value=EPOCH_ISO, range_start="open")` and pass
   `cursor.start_value` (NOT `last_value`) to the API param — per
   PAR-116 trap memo on early-termination patterns.

2. **`scroll_id` stability across runs.** If the cursor is session-bound
   (Elastic-style) it cannot be persisted across Dagster runs, which
   pins us to full re-list each run. If it's a stable opaque token,
   we can resume mid-scan — relevant only once incremental sync is on.

Resources:
- `people`, `companies`, `deals`, `activities` — scroll_id, replace.
  The `activities` resource hits Loxo's `/person_events` endpoint
  (Loxo's URL naming; we keep the friendlier dlt name).
- `jobs` — page-based, replace.
- `users` — single-page list (no pagination metadata documented), replace.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    PageNumberPaginator,
    SinglePagePaginator,
)

from .helpers import LoxoScrollIdPaginator, _client
from .settings import (
    _DEFAULT_DOMAIN,
    ENDPOINT_COMPANIES,
    ENDPOINT_DEALS,
    ENDPOINT_JOBS,
    ENDPOINT_PEOPLE,
    ENDPOINT_PERSON_EVENTS,
    ENDPOINT_USERS,
    JOBS_PAGE_SIZE,
)

Row = dict[str, Any]


@dlt.source(name="loxo")
def loxo_source(
    agency_slug: str = dlt.config.value,
    api_key: str = dlt.secrets.value,
    domain: str = _DEFAULT_DOMAIN,
    base_url: str | None = None,
) -> list[Any]:
    """Loxo source factory — yields six resources covering ATS/CRM data.

    Args:
        agency_slug: Per-agency URL segment (``https://{domain}/api/{agency_slug}/...``).
        api_key: Loxo API token. Resolved from secrets by default.
        domain: Loxo host. Defaults to ``app.loxo.co``; rarely overridden.
        base_url: Test seam — pass the full pre-built base URL to bypass
            ``_base_url`` construction (avoids env-var leakage between
            parallel tests). Production calls leave this ``None``.
    """
    # Test seam: tests pass `base_url=` to bypass `_base_url` construction.
    # Production callers leave `base_url=None`, which routes through `_client`
    # → `_base_url` and the optional `LOXO_API_BASE_URL` env override.
    if base_url is not None:
        client = RESTClient(
            base_url=base_url,
            auth=BearerTokenAuth(api_key),
            headers={"Accept": "application/json"},
        )
    else:
        client = _client(domain, agency_slug, api_key)

    def _scroll(path: str, data_selector: str) -> Iterator[Row]:
        # Loxo's `/companies` and `/deals` reject `per_page` with 422
        # ("Invalid parameters: [:per_page]") even though `/people` and
        # `/person_events` accept it. Inconsistent per-endpoint validation —
        # safest is to omit and let scroll_id pagination handle volume via
        # Loxo's server-side default page size.
        for page in client.paginate(
            path,
            paginator=LoxoScrollIdPaginator(),
            data_selector=data_selector,
        ):
            yield from page

    @dlt.resource(name="people", primary_key="id", write_disposition="replace")
    def people() -> Iterator[Row]:
        yield from _scroll(ENDPOINT_PEOPLE, data_selector="people")

    @dlt.resource(name="jobs", primary_key="id", write_disposition="replace")
    def jobs() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_JOBS,
            params={"per_page": JOBS_PAGE_SIZE},
            paginator=PageNumberPaginator(base_page=1),
            data_selector="jobs",
        ):
            yield from page

    @dlt.resource(name="companies", primary_key="id", write_disposition="replace")
    def companies() -> Iterator[Row]:
        yield from _scroll(ENDPOINT_COMPANIES, data_selector="companies")

    @dlt.resource(name="deals", primary_key="id", write_disposition="replace")
    def deals() -> Iterator[Row]:
        yield from _scroll(ENDPOINT_DEALS, data_selector="deals")

    @dlt.resource(name="activities", primary_key="id", write_disposition="replace")
    def activities() -> Iterator[Row]:
        # Loxo's API names this collection `person_events` (returns 404 on
        # `/activities`). The dlt resource keeps the friendlier name for the
        # downstream warehouse table.
        yield from _scroll(ENDPOINT_PERSON_EVENTS, data_selector="person_events")

    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    def users() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_USERS,
            paginator=SinglePagePaginator(),
            data_selector="users",
        ):
            yield from page

    return [people, jobs, companies, deals, activities, users]


__all__ = ["loxo_source"]
