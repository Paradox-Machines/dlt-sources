"""Notion source helpers — paginators, schema hints, and transform utilities.

Notion API pagination
---------------------
All Notion list endpoints return a top-level envelope:

    {
        "object": "list",
        "results": [...],
        "next_cursor": "<opaque string> | null",
        "has_more": true | false
    }

There are two ways to pass the continuation cursor back:

* **Query-param endpoints** (``GET /v1/users``, ``GET /v1/blocks/{id}/children``,
  ``GET /v1/comments``): cursor goes in the ``start_cursor`` query parameter.
* **Search endpoint** (``POST /v1/search``): cursor goes in the JSON *request
  body* under ``start_cursor``.  dlt's ``JSONResponseCursorPaginator`` supports
  this via ``cursor_body_path``.

Both variants use the same response envelope, so ``cursor_path="next_cursor"``
and ``has_more_path="has_more"`` are identical.

Column hints
------------
dlt drops a column from the Parquet schema entirely when every row in a load
batch has a NULL value for it.  That breaks downstream dbt staging models that
reference those columns by name.  The ``columns()`` helper here matches the
attio pattern — it builds a ``@dlt.resource(columns=...)`` dict of nullable
hints so each declared column is always materialised.

Notion-specific transforms
--------------------------
Notion page ``properties`` are a **dict keyed by workspace-specific property
names** (e.g. ``"Deal Value (ARR)"``, ``"Status"``, ``"Owner"``).  Different
Notion workspaces define entirely different property sets, so dlt's default
``__`` flattening would produce per-workspace column names that cannot be
joined across BUs.  Those fields are therefore stored as JSON strings via
``data_type: json`` column hints; downstream dbt models use DuckDB's
``json_extract_*`` / ``from_json`` to parse them.

The same treatment applies to ``created_by`` and ``last_edited_by``, which
contain actor objects whose exact depth differs between human users (who have
a ``person.email`` sub-key) and bots (which have a ``bot.owner`` sub-tree).
"""

from __future__ import annotations

from typing import Any

from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Schema-hint helpers
# ---------------------------------------------------------------------------


def nullable_column(data_type: str) -> dict[str, Any]:
    """Return a dlt column hint dict for a nullable column of the given type."""
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    timestamp: tuple[str, ...] = (),
    bool_: tuple[str, ...] = (),
    json_: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=…)`` hint map.

    dlt drops all-NULL columns from a batch's Parquet schema, which then
    breaks downstream staging models that reference them.  Up-front hints
    guarantee the column exists regardless of data shape.

    Args:
        text: column names that should be stored as text / varchar.
        timestamp: column names for ISO-8601 datetime strings (stored as
            ``timestamp`` so DuckDB can cast them without a STRPTIME call).
        bool_: column names for boolean flags.
        json_: column names whose values are nested dicts/lists that must be
            stored as opaque JSON strings (e.g. Notion ``properties``).
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in timestamp:
        out[c] = nullable_column("timestamp")
    for c in bool_:
        out[c] = nullable_column("bool")
    for c in json_:
        out[c] = nullable_column("json")
    return out


# ---------------------------------------------------------------------------
# Paginators
# ---------------------------------------------------------------------------


def users_paginator() -> JSONResponseCursorPaginator:
    """Paginator for ``GET /v1/users``.

    Notion returns ``{ results, has_more, next_cursor }``; the cursor is
    passed back as a query param ``start_cursor`` on the next request.
    """
    return JSONResponseCursorPaginator(
        cursor_path="next_cursor",
        cursor_param="start_cursor",
        has_more_path="has_more",
    )


def search_paginator() -> JSONResponseCursorPaginator:
    """Paginator for ``POST /v1/search``.

    The search endpoint is a POST, so the cursor is placed in the *request
    body* under ``start_cursor`` rather than as a query param.  dlt's
    ``JSONResponseCursorPaginator`` supports this via ``cursor_body_path``.
    """
    return JSONResponseCursorPaginator(
        cursor_path="next_cursor",
        cursor_body_path="start_cursor",
        has_more_path="has_more",
    )


def children_paginator() -> JSONResponseCursorPaginator:
    """Paginator for ``GET /v1/blocks/{id}/children``.

    Same query-param cursor shape as ``/v1/users``.
    """
    return JSONResponseCursorPaginator(
        cursor_path="next_cursor",
        cursor_param="start_cursor",
        has_more_path="has_more",
    )


def comments_paginator() -> JSONResponseCursorPaginator:
    """Paginator for ``GET /v1/comments``.

    Same query-param cursor shape as ``/v1/users``.

    Note: Notion comments do **not** expose a monotonic ``updated_at``
    timestamp, so the ``comments`` resource uses ``append`` + dedup-on-id
    at the staging layer rather than an incremental cursor.
    """
    return JSONResponseCursorPaginator(
        cursor_path="next_cursor",
        cursor_param="start_cursor",
        has_more_path="has_more",
    )
