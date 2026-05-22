"""Agree.com source helpers — paginator and schema hints."""

from __future__ import annotations

from typing import Any

from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

Row = dict[str, Any]

# Epoch ISO used as the default incremental cursor start value for contacts.
EPOCH_ISO = "1970-01-01T00:00:00Z"


def agree_paginator() -> PageNumberPaginator:
    """Return a configured PageNumberPaginator for Agree.com list endpoints.

    Agree.com v1 list responses have the shape ``{data, pagination}`` where
    ``pagination.total_pages`` is the total number of pages. Pages are
    1-indexed.
    """
    return PageNumberPaginator(base_page=1, total_path="pagination.total_pages")


def nullable_column(data_type: str) -> dict[str, Any]:
    """Return a single nullable column hint dict."""
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
    boolean: tuple[str, ...] = (),
    timestamp: tuple[str, ...] = (),
    decimal: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=...)`` map of typed nullable column hints.

    dlt drops all-NULL columns from the load schema, which breaks downstream
    consumers that reference those columns. Explicit hints guarantee the column
    is materialised regardless of whether any row carries a non-NULL value.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    for c in boolean:
        out[c] = nullable_column("bool")
    for c in timestamp:
        out[c] = nullable_column("timestamp")
    for c in decimal:
        out[c] = nullable_column("decimal")
    return out
