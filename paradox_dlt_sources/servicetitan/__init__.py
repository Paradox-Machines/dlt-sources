"""ServiceTitan dlt source — multi-tenant field-service data into one warehouse.

One app registration (one shared ``ST-App-Key``) is installed on several
ServiceTitan tenants, each with its own OAuth client id / secret.  The source
loads every tenant into the same tables:

* every row carries ``tenant_id`` + ``tenant_name``;
* every primary key is compound — ``(tenant_id, id)`` — because ServiceTitan ids
  are only unique within a tenant.

Resources
---------
One dlt resource per (endpoint, tenant), named ``{endpoint}_{tenant_id}`` and
writing to the shared ``{endpoint}`` table.  Per-tenant resources give each
tenant its own incremental state, so a quiet tenant never inherits a busier
tenant's high-water mark, and a failure is isolated to one tenant.  The
endpoint table lives in ``settings.ENDPOINTS``.

Pagination and incremental
--------------------------
* ``/export/*`` endpoints walk ``continueFrom`` tokens.  The last token is kept
  in resource state, so the next run resumes from it and only receives records
  changed since.  ``start_date`` seeds the first run (``from=YYYY-MM-DD``).
* Page endpoints walk ``page`` / ``pageSize`` until ``hasMore`` is false.  Those
  that support it are incremental on ``modifiedOn``: the request filter
  ``modifiedOnOrAfter`` is ``cursor.start_value`` — the stable high-water mark
  from the *previous* run — **not** ``cursor.last_value``, which shifts
  mid-extract (repo-wide convention).

Errors
------
A 403 (tenant's app lacks a scope) skips that resource for that tenant with a
warning.  429 responses honour ``Retry-After``.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    ExportPaginator,
    HasMorePagePaginator,
    ServiceTitanAuth,
    TenantConfig,
    columns,
    make_client,
    parse_tenants,
    skip_on_forbidden,
    tag_row,
    tenant_path,
)
from .settings import (
    API_BASE_URL,
    AUTH_BASE_URL,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
    ENDPOINTS_BY_NAME,
    EXPORT_FROM_PARAM,
    INCREMENTAL_CURSOR,
    INCREMENTAL_PARAM,
    PRIMARY_KEY,
    Endpoint,
)

__all__ = ["servicetitan_source"]

logger = logging.getLogger(__name__)

Row = dict[str, Any]

_EXPORT_STATE_KEY = "continue_from"


@dlt.source(name="servicetitan", max_table_nesting=2)
def servicetitan_source(
    tenants: list[dict[str, Any]] = dlt.secrets.value,
    app_key: str = dlt.secrets.value,
    start_date: str | None = None,
    resources: list[str] | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    api_base_url: str = os.environ.get("SERVICETITAN_API_BASE_URL", API_BASE_URL),
    auth_base_url: str = os.environ.get("SERVICETITAN_AUTH_BASE_URL", AUTH_BASE_URL),
) -> list[DltResource]:
    """Load one or more ServiceTitan tenants into shared, tenant-tagged tables.

    Args:
        tenants: One entry per tenant: ``tenant_id``, ``name``, ``client_id``,
            ``client_secret``.
        app_key: The ``ST-App-Key`` of the app, shared by every tenant.
        start_date: ``YYYY-MM-DD`` floor for the first run of incremental
            resources.  ``None`` loads full history.
        resources: Endpoint names to load (e.g. ``["jobs", "invoices"]``).
            ``None`` loads every endpoint in ``settings.ENDPOINTS``.
        page_size: ``pageSize`` for page-paginated endpoints.
        max_attempts: Attempts per request on 429 / 5xx / connection errors.
        api_base_url: API host; override for the integration environment.
        auth_base_url: Auth host; override for the integration environment.

    Returns:
        One resource per (endpoint, tenant), named ``{endpoint}_{tenant_id}``.

    Raises:
        ValueError: On invalid tenant config, ``start_date`` or resource name.
    """
    tenant_configs = parse_tenants(tenants)
    endpoints = _select_endpoints(resources)
    if start_date is not None:
        dt.date.fromisoformat(start_date)

    out: list[DltResource] = []
    for tenant in tenant_configs:
        auth = ServiceTitanAuth(
            client_id=tenant.client_id,
            client_secret=tenant.client_secret,
            app_key=app_key,
            auth_base_url=auth_base_url,
        )
        client = make_client(auth, api_base_url, max_attempts=max_attempts)
        out.extend(
            _make_resource(endpoint, tenant, client, start_date=start_date, page_size=page_size)
            for endpoint in endpoints
        )
    return out


def _select_endpoints(resources: Sequence[str] | None) -> list[Endpoint]:
    """Resolve the ``resources`` filter against the endpoint table."""
    if resources is None:
        return list(ENDPOINTS)
    unknown = sorted(set(resources) - set(ENDPOINTS_BY_NAME))
    if unknown:
        raise ValueError(
            f"servicetitan: unknown resource(s) {unknown}; known: {sorted(ENDPOINTS_BY_NAME)}"
        )
    return [e for e in ENDPOINTS if e.name in set(resources)]


def _make_resource(
    endpoint: Endpoint,
    tenant: TenantConfig,
    client: RESTClient,
    *,
    start_date: str | None,
    page_size: int,
) -> DltResource:
    """Build the dlt resource reading one endpoint for one tenant."""
    resource_kwargs: Mapping[str, Any] = {
        "name": f"{endpoint.name}_{tenant.tenant_id}",
        "table_name": endpoint.name,
        "primary_key": PRIMARY_KEY,
        "write_disposition": "merge",
        "columns": columns(endpoint),
    }

    if endpoint.pagination == "export":

        def export_rows() -> Iterator[list[Row]]:
            yield from skip_on_forbidden(
                _export_pages(client, endpoint, tenant, start_date),
                endpoint=endpoint,
                tenant=tenant,
            )

        return dlt.resource(export_rows, **resource_kwargs)

    if endpoint.incremental:
        initial_value = f"{start_date}T00:00:00Z" if start_date else None

        def incremental_rows(
            cursor: dlt.sources.incremental[str] = dlt.sources.incremental(  # noqa: B008
                INCREMENTAL_CURSOR,
                initial_value=initial_value,
                on_cursor_value_missing="include",
            ),
        ) -> Iterator[list[Row]]:
            params = dict(endpoint.params)
            if cursor.start_value:
                params[INCREMENTAL_PARAM] = cursor.start_value
            yield from skip_on_forbidden(
                _list_pages(client, endpoint, tenant, params, page_size),
                endpoint=endpoint,
                tenant=tenant,
            )

        return dlt.resource(incremental_rows, **resource_kwargs)

    def list_rows() -> Iterator[list[Row]]:
        yield from skip_on_forbidden(
            _list_pages(client, endpoint, tenant, dict(endpoint.params), page_size),
            endpoint=endpoint,
            tenant=tenant,
        )

    return dlt.resource(list_rows, **resource_kwargs)


def _export_pages(
    client: RESTClient,
    endpoint: Endpoint,
    tenant: TenantConfig,
    start_date: str | None,
) -> Iterator[list[Row]]:
    """Walk an ``/export/*`` endpoint from the saved continuation token.

    The latest ``continueFrom`` is written to resource state after every page;
    dlt commits state only with a successful load, so an interrupted run
    restarts from the previous run's token.
    """
    state = dlt.current.resource_state()
    start = state.get(_EXPORT_STATE_KEY) or start_date
    params = dict(endpoint.params)
    if start:
        params[EXPORT_FROM_PARAM] = start
    paginator = ExportPaginator(start)
    for page in client.paginate(
        tenant_path(endpoint, tenant.tenant_id), params=params, paginator=paginator
    ):
        yield [tag_row(row, tenant) for row in page]
        if paginator.continue_from:
            state[_EXPORT_STATE_KEY] = paginator.continue_from


def _list_pages(
    client: RESTClient,
    endpoint: Endpoint,
    tenant: TenantConfig,
    params: dict[str, Any],
    page_size: int,
) -> Iterator[list[Row]]:
    """Walk a page/pageSize endpoint until ``hasMore`` is false."""
    for page in client.paginate(
        tenant_path(endpoint, tenant.tenant_id),
        params=params,
        paginator=HasMorePagePaginator(page_size),
    ):
        yield [tag_row(row, tenant) for row in page]
