"""apollo_io source helpers — paginator + any REST-client builders.

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


class ApolloIoPagePaginator(BasePaginator):
    """Simple page-number paginator for Apollo.io GET endpoints.

    Advances ``?page=N`` until an empty page is returned.
    Used for low-volume GET endpoints (email_accounts, labels).
    """

    def __init__(self, per_page: int = 100) -> None:
        super().__init__()
        self._page: int = 1
        self._per_page: int = per_page

    def update_state(
        self,
        response: Response,
        data: list[Any] | None = None,
    ) -> None:
        if not data:
            self._has_next_page = False
        else:
            self._page += 1

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}
        request.params["page"] = self._page
        request.params["per_page"] = self._per_page
