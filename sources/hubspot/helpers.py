"""HubSpot source helpers — paginators and schema hints."""

from __future__ import annotations

from typing import Any

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


def nullable_column(data_type: str) -> dict[str, Any]:
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=...)`` map of nullable text + bigint hints.

    dlt drops all-NULL columns from the load schema, breaking downstream
    consumers that reference them.  Up-front hints guarantee the column
    exists regardless of data shape.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    return out


class EngagementsOffsetPaginator(BasePaginator):
    """Engagements v1 ``{results, hasMore, offset}`` pagination.

    The legacy ``/engagements/v1/engagements/paged`` endpoint emits the
    next-page token as the ``offset`` field in the response body, and
    signals more pages via ``hasMore: true``.  dlt's stock
    ``OffsetPaginator`` increments offset client-side by ``limit``, which
    only works for dense pages.  This paginator reads the server-supplied
    offset directly, which is the correct approach for the v1 endpoint.
    """

    def __init__(self) -> None:
        super().__init__()
        self._offset: int | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body: dict[str, Any] = response.json()
        if body.get("hasMore") and "offset" in body:
            self._offset = int(body["offset"])
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._offset is None:
            return
        if request.params is None:
            request.params = {}
        request.params["offset"] = self._offset
