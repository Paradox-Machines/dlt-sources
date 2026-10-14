"""ServiceTitan source — auth, pagination, tenant config, and row transforms."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import requests
from dlt.sources.helpers.requests import Client
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import PreparedRequest, Request, Response
from requests.auth import AuthBase
from requests.exceptions import HTTPError

from .settings import (
    API_VERSION,
    APP_KEY_HEADER,
    BASE_COLUMNS,
    DATA_SELECTOR,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_TOKEN_TTL_SECONDS,
    EXPORT_FROM_PARAM,
    HTTP_FORBIDDEN,
    MAX_RETRY_DELAY_SECONDS,
    PATH_TEMPLATE,
    PATH_TOKEN,
    TOKEN_REFRESH_SAFETY_SECONDS,
    Endpoint,
)

LOGGER = logging.getLogger(__name__)

Row = dict[str, Any]

_TENANT_KEYS = ("tenant_id", "name", "client_id", "client_secret")


# ---------------------------------------------------------------------------
# Tenant config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TenantConfig:
    """Credentials and identity of one ServiceTitan tenant.

    Attributes:
        tenant_id: ServiceTitan tenant id (the ``{tenant}`` path segment).
        name: Human-readable operating-company name, stamped on every row.
        client_id: OAuth client id issued to the app for this tenant.
        client_secret: OAuth client secret issued to the app for this tenant.
    """

    tenant_id: str
    name: str
    client_id: str
    client_secret: str

    def __repr__(self) -> str:
        """Return a repr that never leaks the client secret."""
        return f"TenantConfig(tenant_id={self.tenant_id!r}, name={self.name!r})"


def parse_tenants(tenants: Sequence[Mapping[str, Any]]) -> list[TenantConfig]:
    """Validate and normalise the ``tenants`` config list.

    Args:
        tenants: One mapping per tenant with ``tenant_id``, ``name``,
            ``client_id`` and ``client_secret``.

    Returns:
        One ``TenantConfig`` per entry, in config order.

    Raises:
        ValueError: If the list is empty, an entry misses a key, or two entries
            share a ``tenant_id``.
    """
    if not tenants:
        raise ValueError("servicetitan: `tenants` must list at least one tenant")
    parsed: list[TenantConfig] = []
    seen: set[str] = set()
    for index, entry in enumerate(tenants):
        missing = [k for k in _TENANT_KEYS if entry.get(k) in (None, "")]
        if missing:
            raise ValueError(f"servicetitan: tenants[{index}] is missing {', '.join(missing)}")
        tenant = TenantConfig(*(str(entry[k]) for k in _TENANT_KEYS))
        if tenant.tenant_id in seen:
            raise ValueError(f"servicetitan: duplicate tenant_id {tenant.tenant_id!r}")
        seen.add(tenant.tenant_id)
        parsed.append(tenant)
    return parsed


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class ServiceTitanAuth(AuthBase):
    """OAuth 2.0 client-credentials bearer auth plus the ``ST-App-Key`` header.

    The access token is fetched lazily from ``{auth_base_url}/connect/token``
    and cached until ``TOKEN_REFRESH_SAFETY_SECONDS`` before its expiry.

    Args:
        client_id: Tenant-specific OAuth client id.
        client_secret: Tenant-specific OAuth client secret.
        app_key: The app key shared by every tenant.
        auth_base_url: ServiceTitan auth host.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        app_key: str,
        auth_base_url: str,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._app_key = app_key
        self._token_url = f"{auth_base_url.rstrip('/')}{PATH_TOKEN}"
        self._access_token: str | None = None
        self._expires_at = 0.0

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Attach the bearer token and app key, refreshing the token if stale."""
        request.headers["Authorization"] = f"Bearer {self.access_token()}"
        request.headers[APP_KEY_HEADER] = self._app_key
        return request

    def access_token(self) -> str:
        """Return a valid access token, fetching a new one when needed."""
        if (
            self._access_token is None
            or time.time() >= self._expires_at - TOKEN_REFRESH_SAFETY_SECONDS
        ):
            self._refresh_access_token()
        assert self._access_token is not None
        return self._access_token

    def _refresh_access_token(self) -> None:
        """Fetch and cache a fresh access token."""
        response = requests.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = str(body["access_token"])
        self._expires_at = time.time() + float(body.get("expires_in", DEFAULT_TOKEN_TTL_SECONDS))


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class ExportPaginator(BasePaginator):
    """Continuation-token paginator for ``/export/*`` endpoints.

    Each response returns ``hasMore`` and ``continueFrom``; the next request
    sends that token as ``from``.  ``continue_from`` holds the latest token so
    the caller can persist it and resume on the next run.

    Args:
        continue_from: Token (or ``YYYY-MM-DD`` date) the walk started from.
    """

    def __init__(self, continue_from: str | None = None) -> None:
        super().__init__()
        self.continue_from = continue_from

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        """Read ``hasMore`` / ``continueFrom`` from the response body."""
        body = response.json()
        token = body.get("continueFrom")
        if token:
            self.continue_from = str(token)
        self._has_next_page = bool(body.get("hasMore")) and bool(token)

    def update_request(self, request: Request) -> None:
        """Send the latest continuation token as ``from``."""
        if request.params is None:
            request.params = {}
        request.params[EXPORT_FROM_PARAM] = self.continue_from


class HasMorePagePaginator(BasePaginator):
    """``page``/``pageSize`` paginator that stops when ``hasMore`` is false.

    Args:
        page_size: Rows requested per page.
    """

    def __init__(self, page_size: int) -> None:
        super().__init__()
        self.page = 1
        self.page_size = page_size

    def init_request(self, request: Request) -> None:
        """Seed ``page`` and ``pageSize`` on the first request."""
        if request.params is None:
            request.params = {}
        request.params["page"] = self.page
        request.params["pageSize"] = self.page_size

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        """Advance when the response reports ``hasMore``."""
        self._has_next_page = bool(response.json().get("hasMore")) and bool(data)
        if self._has_next_page:
            self.page += 1

    def update_request(self, request: Request) -> None:
        """Request the next page number."""
        if request.params is None:
            request.params = {}
        request.params["page"] = self.page


# ---------------------------------------------------------------------------
# REST client
# ---------------------------------------------------------------------------


def make_client(
    auth: AuthBase,
    base_url: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> RESTClient:
    """Return a ``RESTClient`` for the ServiceTitan API.

    The session retries 429 and 5xx responses; a 429 waits for the server's
    ``Retry-After`` header (capped at ``MAX_RETRY_DELAY_SECONDS``).

    Args:
        auth: Normally a ``ServiceTitanAuth``.
        base_url: API host.
        max_attempts: Attempts per request before giving up.
    """
    session = Client(
        raise_for_status=False,
        request_max_attempts=max_attempts,
        request_max_retry_delay=MAX_RETRY_DELAY_SECONDS,
        respect_retry_after_header=True,
    ).session
    return RESTClient(
        base_url=base_url,
        auth=auth,
        session=session,
        data_selector=DATA_SELECTOR,
    )


def tenant_path(endpoint: Endpoint, tenant_id: str) -> str:
    """Return the tenant-scoped API path of an endpoint.

    Args:
        endpoint: Endpoint table entry.
        tenant_id: ServiceTitan tenant id.
    """
    return PATH_TEMPLATE.format(
        module=endpoint.module, version=API_VERSION, tenant=tenant_id, path=endpoint.path
    )


# ---------------------------------------------------------------------------
# Row transforms / schema hints
# ---------------------------------------------------------------------------


def tag_row(row: Row, tenant: TenantConfig) -> Row:
    """Stamp a record with its tenant so ids stay unique across tenants.

    Args:
        row: Raw API record.
        tenant: Tenant the record was read from.
    """
    return {**row, "tenant_id": tenant.tenant_id, "tenant_name": tenant.name}


def columns(endpoint: Endpoint) -> dict[str, dict[str, Any]]:
    """Build nullable column hints for an endpoint's table.

    Each column gets its own dict — dlt mutates hints in place, so sharing one
    dict across columns silently merges them.

    Args:
        endpoint: Endpoint table entry.
    """
    hints = {**BASE_COLUMNS, **endpoint.columns}
    return {name: {"data_type": dtype, "nullable": True} for name, dtype in hints.items()}


def skip_on_forbidden(
    gen: Iterator[Any], *, endpoint: Endpoint, tenant: TenantConfig
) -> Iterator[Any]:
    """Yield from ``gen``, but log and stop on HTTP 403.

    A tenant's app registration can lack the scope for one endpoint.  Failing
    the resource would abort the whole source, so the other tenants and
    resources would not load either.

    Args:
        gen: The page generator to guard.
        endpoint: Endpoint being read.
        tenant: Tenant being read.
    """
    try:
        yield from gen
    except HTTPError as exc:
        response = getattr(exc, "response", None)
        if response is None or response.status_code != HTTP_FORBIDDEN:
            raise
        LOGGER.warning(
            "ServiceTitan tenant %s (%s): %r returned 403 Forbidden — the app lacks "
            "the %r scope for this tenant. Skipping this resource for this tenant; "
            "other tenants and resources continue.",
            tenant.name,
            tenant.tenant_id,
            endpoint.name,
            endpoint.scope,
        )
