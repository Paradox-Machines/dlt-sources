"""monday_crm source helpers — paginator + any REST-client builders.

The paginator's ``update_request`` mutates ``request.params`` / ``request.json``
on a ``requests.Request`` (the pre-send object), NEVER a ``PreparedRequest`` —
this matches dlt's ``BasePaginator`` contract (see CHANGELOG 0.1.0a2 fix and the
loxo ``LoxoScrollIdPaginator``). Mutating the URL string instead would
accumulate duplicate query keys across pages.
"""

from __future__ import annotations

from typing import Any

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


class MondayPagePaginator(BasePaginator):
    """Page-number paginator for monday.com GraphQL resources.

    Injects ``page`` into the GraphQL ``variables`` dict on each request and
    stops when the returned list is shorter than ``limit`` (terminal page).
    """

    def __init__(self, limit: int = 50) -> None:
        super().__init__()
        self._limit: int = limit
        self._page: int = 1

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        if not data or len(data) < self._limit:
            self._has_next_page = False
        else:
            self._page += 1

    def update_request(self, request: Request) -> None:
        body: dict[str, Any] = {}
        if request.json:
            body = dict(request.json)
        body.setdefault("variables", {})["page"] = self._page
        body["variables"]["limit"] = self._limit
        request.json = body


class MondayItemsPaginator(BasePaginator):
    """Cursor paginator for monday.com items_page GraphQL query.

    Reads the ``cursor`` field from ``data.boards[0].items_page.cursor`` in
    the response JSON and injects it into the GraphQL ``variables`` on the
    next request. Stops when the cursor is absent or null.
    """

    def __init__(self, limit: int = 100) -> None:
        super().__init__()
        self._limit: int = limit
        self._next_cursor: str | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        try:
            body: Any = response.json()
            boards: list[Any] = (body.get("data") or {}).get("boards") or []
            if boards:
                items_page: dict[str, Any] = boards[0].get("items_page") or {}
                cursor: Any = items_page.get("cursor")
                if cursor:
                    self._next_cursor = str(cursor)
                    self._has_next_page = True
                    return
        except Exception:  # noqa: BLE001
            pass
        self._next_cursor = None
        self._has_next_page = False

    def update_request(self, request: Request) -> None:
        body: dict[str, Any] = {}
        if request.json:
            body = dict(request.json)
        body.setdefault("variables", {})["limit"] = self._limit
        if self._next_cursor is not None:
            body["variables"]["cursor"] = self._next_cursor
        else:
            body["variables"].pop("cursor", None)
        request.json = body
