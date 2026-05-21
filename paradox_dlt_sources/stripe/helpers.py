"""Stripe source helpers — paginator, row transforms, schema hints."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pendulum
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Schema-hint builder (inlined from _common — no internal-repo dependency)
# ---------------------------------------------------------------------------


def nullable_column(data_type: str) -> dict[str, Any]:
    """Return a single nullable column hint dict."""
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


# ---------------------------------------------------------------------------
# Paginator
# ---------------------------------------------------------------------------


class StripeCursorPaginator(BasePaginator):
    """Paginate Stripe-style: ``has_more`` flag + ``starting_after=<last_id>``.

    Stripe's REST API does not return a ``next`` URL — instead, every response
    carries a ``has_more`` boolean and the client must pass
    ``starting_after=<last_item_id>`` to fetch the next page.  Once
    ``has_more`` is false (or the page is empty), pagination stops.
    """

    def __init__(self) -> None:
        super().__init__()
        self._cursor: str | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body = response.json()
        if body.get("has_more") and data:
            self._cursor = data[-1]["id"]
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._cursor is None:
            return
        params = dict(request.params or {})
        params["starting_after"] = self._cursor
        request.params = params


# ---------------------------------------------------------------------------
# Row transforms
# ---------------------------------------------------------------------------


def coerce_epoch_to_timestamp(field_name: str) -> Callable[[Row], Row]:
    """Build an ``add_map`` row transform that converts an epoch int to a datetime.

    Stripe's ``created`` field is a Unix epoch integer.  Coercing to a real
    datetime lets parquet infer ``timestamp`` instead of ``int64`` and keeps
    the incremental cursor comparison type-stable.
    """

    def _coerce(row: Row) -> Row:
        value = row.get(field_name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return row
        return {**row, field_name: pendulum.from_timestamp(value)}

    return _coerce


def hoist_invoice_subscription(row: Row) -> Row:
    """Hoist ``parent.subscription_details.subscription`` to top-level.

    Stripe deprecated the top-level ``subscription`` field on Invoice; the
    subscription id now lives at ``parent.subscription_details.subscription``.
    Hoisting it back preserves dbt's ``subscription as subscription_id``
    reference without raising ``max_table_nesting``.
    """
    if row.get("subscription") is not None:
        return row
    parent = row.get("parent") or {}
    sub_details = parent.get("subscription_details") or {}
    sub = sub_details.get("subscription")
    if sub is None:
        return row
    return {**row, "subscription": sub}
