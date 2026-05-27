"""Loxo source helpers — scroll-id paginator + REST client builder."""

from __future__ import annotations

import os
from typing import Any

from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


def _base_url(domain: str, agency_slug: str) -> str:
    """Construct the per-agency API base URL, honoring ``LOXO_API_BASE_URL`` env override.

    Loxo's hosted plan serves every agency from ``https://app.loxo.co/api/{slug}``.
    Custom-domain agencies override the full URL via ``LOXO_API_BASE_URL``.
    """
    override = os.environ.get("LOXO_API_BASE_URL")
    if override:
        return override.rstrip("/")
    return f"https://{domain}/api/{agency_slug}"


class LoxoScrollIdPaginator(BasePaginator):
    """Cursor pagination via ``scroll_id`` query param.

    Loxo returns ``scroll_id`` on listing responses for candidates, companies,
    deals, and activities. To fetch the next page, pass that value back as
    ``scroll_id`` on the next request. The loop ends when the field is absent
    or the page is empty.

    Mutates ``request.params`` (not the URL string) — same rule as dlt's
    built-in cursor paginators. Otherwise ``scroll_id=`` accumulates as
    duplicate keys across pages.
    """

    def __init__(self) -> None:
        super().__init__()
        self._scroll_id: str | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body = response.json() if response.content else {}
        next_scroll = body.get("scroll_id")
        # Treat both "no scroll_id" and "empty page" as terminal.
        if next_scroll and data:
            self._scroll_id = next_scroll
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._scroll_id is None:
            return
        params = dict(request.params or {})
        params["scroll_id"] = self._scroll_id
        request.params = params


def _client(domain: str, agency_slug: str, api_key: str) -> RESTClient:
    """Build a configured ``RESTClient`` for a given agency."""
    return RESTClient(
        base_url=_base_url(domain, agency_slug),
        auth=BearerTokenAuth(api_key),
        headers={"Accept": "application/json"},
    )
