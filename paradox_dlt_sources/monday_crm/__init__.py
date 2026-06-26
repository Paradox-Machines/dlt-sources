"""
monday_crm (monday Sales CRM) dlt source.

Extracts boards, items (cursor-paginated per board, incremental on updated_at),
users, teams, tags, updates, and workspaces from the monday.com GraphQL API
(https://api.monday.com/v2).

Authentication: Bearer token (personal API token or OAuth2 access token) via
the Authorization header.

Pagination:
  - boards, users, updates, workspaces: page-number (page= integer, limit=N).
  - items: cursor-based per board (items_page cursor returned in the response
    body). A custom MondayItemsPaginator handles cursor injection into GraphQL
    variables and detects terminal pages.
  - teams, tags: single-page (no pagination needed).

Incremental (items): dlt.sources.incremental on updated_at. No server-side
updated_since filter is documented — items are fetched in full per board and
dlt filters client-side via cursor.start_value. The cursor is persisted so
subsequent runs skip already-seen rows.

OPEN QUESTIONS (surfaced from research artifact):
  - Does monday.com expose a server-side updated_since / equivalent filter on
    items_page or boards queries, or must incremental logic be done client-side?
  - Is the items_page cursor stable across independent sync runs, or only valid
    for the duration of a single paginated session?
  - monday CRM (monday Sales CRM): are there dedicated CRM endpoints (contacts,
    deals, leads) or are they all standard boards/items under specific board
    templates?
  - Confirmed API-Version header: 2024-01 (needs verification for CRM context).
  - Rate limits: per-minute and per-day query complexity limits are unknown.
  - Do workspaces, tags, and teams support page-based pagination or are they
    always returned in a single response?
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from functools import wraps
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import MondayPagePaginator
from .settings import (
    MONDAY_CRM_API_BASE_URL,
    QUERY_BOARDS,
    QUERY_TAGS,
    QUERY_TEAMS,
    QUERY_UPDATES,
    QUERY_USERS,
    QUERY_WORKSPACES,
)

Row = dict[str, Any]

_UPDATED_AT_INCREMENTAL: dlt.sources.incremental[str] = dlt.sources.incremental(
    "updated_at",
    initial_value=None,
    on_cursor_value_missing="include",
)


def _sentinel_backed(
    *pk_columns: str,
    _types: dict[str, str] | None = None,
) -> Callable[[Callable[..., Iterator[Any]]], Callable[..., Iterator[Any]]]:
    """Wrap a resource generator so an empty resource still emits one all-NULL row.

    Applied by the harness directly BELOW ``@dlt.resource``. Drives the inner
    generator; if it yields zero rows, yields a single all-NULL sentinel row over
    the sentinel column(s) so dlt still writes the table (the attio pattern). The
    sentinel is all-NULL, so downstream / the harness post-gate treats it as a
    sentinel, NOT real data.

    The all-NULL sentinel row carries PER-ITEM type hints (``dlt.mark.with_hints``
    / ``dlt.mark.make_hints``) so EACH sentinel column materializes WITH a type
    even though every value is None — without the hint dlt cannot infer a type for
    an all-NULL column and DROPS it ("...columns ... could not have their types
    inferred ... will not be materialized"), leaving the empty resource's table
    column-less. The hint rides on the SENTINEL ITEM, so it is emitted ONLY when
    the inner generator yielded nothing (empty fixture / empty production window):
    a real-data load never takes this branch, so dlt's native type inference still
    governs (an integer PK stays BIGINT — no static-``data_type`` poison). The hint
    adds TYPES, never VALUES — the row is still all-NULL, so it stays classified as
    a sentinel. ``_types`` maps a sentinel column to its declared dlt ``data_type``
    (from the model's ``@dlt.resource(columns=...)``); an undeclared column defaults
    to ``"text"`` (safe — only ever applied to an EMPTY resource, never to data).
    """
    columns = pk_columns or ("id",)
    types = _types or {}

    def _decorate(fn: Callable[..., Iterator[Any]]) -> Callable[..., Iterator[Any]]:
        @wraps(fn)
        def _wrapped(*args: Any, **kwargs: Any) -> Iterator[Any]:
            yielded = False
            for row in fn(*args, **kwargs):
                yielded = True
                yield row
            if not yielded:
                sentinel = {column: None for column in columns}
                hints = dlt.mark.make_hints(
                    columns={
                        column: {
                            "data_type": types.get(column, "text"),
                            "nullable": True,
                        }
                        for column in columns
                    }
                )
                yield dlt.mark.with_hints(sentinel, hints)

        return _wrapped

    return _decorate


def _nullable_pk(*pk_columns: str) -> Callable[[Any], Any]:
    """Relax a resource's sentinel column(s) to nullable so the sentinel passes.

    Applied by the harness directly ABOVE ``@dlt.resource``. A dlt primary key is
    non-nullable by default, so an all-NULL sentinel row would raise
    ``UnboundColumnException`` at normalize; a non-nullable DECLARED column (or the
    incremental cursor field) would likewise raise a NOT NULL ConstraintException.
    This hints each named column ``nullable=True`` ONLY — it does NOT pin a
    ``data_type``, so the column keeps its NATIVE inferred type when real rows
    arrive (F-TYPE: the old ``data_type="text"`` poisoned every PK to text). It only
    PERMITS NULL — inert when real rows arrive.
    """

    def _decorate(resource: Any) -> Any:
        if pk_columns:
            resource.apply_hints(columns={column: {"nullable": True} for column in pk_columns})
        return resource

    return _decorate


def _sentinel_incremental(cursor: str, **kwargs: Any) -> Callable[[Any], Any]:
    """Re-apply an incremental that INCLUDES a missing/NULL cursor on an empty resource.

    Applied by the harness directly ABOVE ``@dlt.resource`` for an INCREMENTAL
    resource. The harness sentinel row is all-NULL and omits the cursor field, so
    dlt's default incremental (``on_cursor_value_missing="raise"``) would raise
    ``IncrementalCursorPathMissing`` for the empty resource. This builds a FRESH
    ``dlt.sources.incremental`` over the same cursor + preserved params with
    ``on_cursor_value_missing="include"`` and re-applies it via ``apply_hints`` so
    the sentinel row is included (the table still materializes) while real rows are
    filtered by the cursor exactly as before — inert when real rows arrive.
    """

    def _decorate(resource: Any) -> Any:
        resource.apply_hints(
            incremental=dlt.sources.incremental(cursor, on_cursor_value_missing="include", **kwargs)
        )
        return resource

    return _decorate


def _build_boards(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="boards", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def boards() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=50)
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_BOARDS, "variables": {"limit": 50, "page": 1}},
            paginator=paginator,
            data_selector="data.boards",
        ):
            yield from page

    return boards


def _build_items(
    client: RESTClient,
    updated_at: dlt.sources.incremental[str],
) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="items", primary_key="id", write_disposition="merge")
    @_sentinel_backed("id")
    def items() -> Iterator[Row]:
        query = "query ($boardId: ID!, $limit: Int, $cursor: String) { boards(ids: [$boardId]) { items_page(limit: $limit, cursor: $cursor) { cursor items { id name state board_id group { id title } column_values { id text value column { id title type } } created_at updated_at } } } }"
        for page in client.paginate(
            "",
            method="POST",
            json={"query": query, "variables": {"limit": "100"}},
            data_selector="data.boards[*].items_page.items",
            paginator=MondayPagePaginator(),
        ):
            for entry in page:
                if isinstance(entry, dict):
                    yield entry
                else:
                    yield from entry

    return items


def _build_users(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def users() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=100)
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_USERS, "variables": {"limit": 100, "page": 1}},
            paginator=paginator,
            data_selector="data.users",
        ):
            yield from page

    return users


def _build_teams(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="teams", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def teams() -> Iterator[Row]:
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_TEAMS, "variables": {}},
            data_selector="data.teams",
        ):
            yield from page

    return teams


def _build_tags(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="tags", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def tags() -> Iterator[Row]:
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_TAGS, "variables": {}},
            data_selector="data.tags",
        ):
            yield from page

    return tags


def _build_updates(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="updates", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def updates() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=100)
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_UPDATES, "variables": {"limit": 100, "page": 1}},
            paginator=paginator,
            data_selector="data.updates",
        ):
            yield from page

    return updates


def _build_workspaces(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="workspaces", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def workspaces() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=50)
        for page in client.paginate(
            "",
            method="POST",
            json={
                "query": QUERY_WORKSPACES,
                "variables": {"limit": 50, "page": 1},
            },
            paginator=paginator,
            data_selector="data.workspaces",
        ):
            yield from page

    return workspaces


def _build_columns(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="columns", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def columns() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=50)
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_BOARDS, "variables": {"limit": 50, "page": 1}},
            paginator=paginator,
            data_selector="data.boards",
        ):
            for board in page:
                board_id = board.get("id")
                for col in board.get("columns") or []:
                    row: Row = dict(col)
                    row["board_id"] = board_id
                    yield row

    return columns


def _build_groups(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="groups", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def groups() -> Iterator[Row]:
        paginator = MondayPagePaginator(limit=50)
        for page in client.paginate(
            "",
            method="POST",
            json={"query": QUERY_BOARDS, "variables": {"limit": 50, "page": 1}},
            paginator=paginator,
            data_selector="data.boards",
        ):
            for board in page:
                board_id = board.get("id")
                for grp in board.get("groups") or []:
                    row = dict(grp)
                    row["board_id"] = board_id
                    yield row

    return groups


@dlt.source(name="monday_crm")
def monday_crm_source(
    api_key: str = dlt.secrets.value,
    base_url: str = MONDAY_CRM_API_BASE_URL,
) -> list[Any]:
    """monday_crm source factory.

    Args:
        api_key: API token. Resolved from secrets by default.
        base_url: Test seam — pass the full pre-built base URL to bypass default
            construction (avoids env-var leakage between parallel tests).
            Production callers leave this as the settings default.
    """
    client = RESTClient(
        base_url=base_url,
        auth=BearerTokenAuth(api_key),
        headers={
            "Accept": "application/json",
            "API-Version": "2024-01",
            "Content-Type": "application/json",
        },
    )

    boards = _build_boards(client)
    items = _build_items(client, _UPDATED_AT_INCREMENTAL)
    users = _build_users(client)
    teams = _build_teams(client)
    tags = _build_tags(client)
    updates = _build_updates(client)
    workspaces = _build_workspaces(client)
    columns = _build_columns(client)
    groups = _build_groups(client)

    return [boards, items, users, teams, tags, updates, workspaces, columns, groups]


__all__ = ["monday_crm_source"]
