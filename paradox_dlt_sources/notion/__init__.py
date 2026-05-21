"""Notion dlt source — users, databases, pages, blocks, comments.

Auth
----
Notion uses **internal integration tokens** (``Authorization: Bearer <token>``).
Each BU mints its own token from the Notion workspace → Integrations page and
shares it with the pages/databases it wants synced.

The API also requires a ``Notion-Version`` date header on every request;
``settings.NOTION_API_VERSION`` is pinned to the same release the legacy
Airbyte connector used so downstream dbt field surfaces remain stable.

Resources
---------
* ``users`` — workspace members; full replace each sync (small, static set).
* ``databases`` — ``POST /v1/search?filter=database``, sorted by
  ``last_edited_time DESC``, incremental with early-termination once items
  older than the prior high-water mark are encountered.
* ``pages`` — same pattern as ``databases`` but ``filter=page``.  Each row
  gets a ``page_id`` alias column so downstream transformers can join on it.
* ``blocks`` — transformer fed by ``pages``; walks
  ``GET /v1/blocks/{page_id}/children`` for every page.  Unshared pages
  return 404 and are silently skipped.
* ``comments`` — transformer fed by ``blocks``; calls
  ``GET /v1/comments?block_id=<id>``.  Comments have no monotonic update
  timestamp — resource uses ``append`` + dedup-on-id at staging.

Pagination
----------
Notion cursor-based pagination uses the ``{ results, has_more, next_cursor }``
envelope.  The search endpoint (POST) passes the cursor in the request body;
all GET endpoints use a ``start_cursor`` query param.  See ``helpers.py`` for
the per-endpoint paginator factories.

Column hints
------------
dlt silently drops a column from the Parquet schema when an entire load batch
has all-NULL values for it.  Every resource declares explicit ``columns=``
hints so the staging layer always sees the expected column set.

``properties``, ``created_by``, and ``last_edited_by`` on pages are stored as
opaque JSON strings (``data_type: json``) because they contain
workspace-specific keys that differ across BUs and cannot be joined via a
shared flat schema.  Downstream dbt uses DuckDB ``json_extract_*`` to parse
them.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from requests.exceptions import HTTPError

from .helpers import (
    children_paginator,
    columns,
    comments_paginator,
    search_paginator,
    users_paginator,
)
from .settings import (
    EPOCH_ISO,
    HTTP_NOT_FOUND,
    NOTION_API_BASE_URL,
    NOTION_API_VERSION,
    PATH_BLOCKS_CHILDREN,
    PATH_COMMENTS,
    PATH_SEARCH,
    PATH_USERS,
)

logger = logging.getLogger(__name__)

Row = dict[str, Any]

# ---------------------------------------------------------------------------
# Column hint maps — one per resource.
#
# Nested Notion fields (e.g. `parent.type`) arrive after dlt normalisation as
# flattened `parent__type` columns; list them under the post-flatten name.
# ---------------------------------------------------------------------------

_USERS_COLUMNS = columns(
    text=(
        "id",
        "type",
        "name",
        "avatar_url",
        "person__email",
        "bot__owner__type",
    ),
)

_DATABASES_COLUMNS = columns(
    text=(
        "id",
        "object",
        "url",
        "parent__type",
        "parent__page_id",
    ),
    timestamp=(
        "created_time",
        "last_edited_time",
    ),
    bool_=("archived",),
    json_=("parent__workspace",),
)

_PAGES_COLUMNS = columns(
    text=(
        "id",
        "object",
        "url",
        "parent__type",
        "parent__database_id",
        "parent__page_id",
    ),
    timestamp=(
        "created_time",
        "last_edited_time",
    ),
    bool_=("archived",),
    # `properties` is a dict keyed by workspace-specific property names
    # ("Deal", "Status", "Deal Value (ARR)", …). Different Notion workspaces
    # define different property sets, so flattening via dlt's `__` separator
    # would create per-workspace columns that are un-joinable across BUs.
    # `json` data_type tells dlt to write the whole sub-tree as a JSON string
    # column, which dbt then parses with DuckDB's `json_extract_*`.
    # Same treatment for `created_by` / `last_edited_by`: their exact depth
    # differs between human users (with `person.email`) and bots
    # (with `bot.owner`).
    json_=(
        "properties",
        "created_by",
        "last_edited_by",
    ),
)

_BLOCKS_COLUMNS = columns(
    text=(
        "id",
        "object",
        "type",
        "page_id",
        "parent__type",
        "parent__page_id",
        "parent__block_id",
    ),
    timestamp=(
        "created_time",
        "last_edited_time",
    ),
    bool_=(
        "archived",
        "has_children",
    ),
)

_COMMENTS_COLUMNS = columns(
    text=(
        "id",
        "object",
        "discussion_id",
        "block_id",
        "parent__type",
        "parent__page_id",
        "parent__block_id",
        "created_by__id",
    ),
    timestamp=(
        "created_time",
        "last_edited_time",
    ),
)


@dlt.source(name="notion")
def notion_source(
    integration_token: str = dlt.secrets.value,
    base_url: str = os.environ.get("NOTION_API_BASE_URL", NOTION_API_BASE_URL),
) -> list[Any]:
    """Notion source factory — yields users, databases, pages, blocks, comments.

    Args:
        integration_token: Notion internal integration token (Bearer auth).
            Resolved from ``.dlt/secrets.toml`` → ``[sources.notion]`` by
            default.
        base_url: Notion API base URL.  Override in tests to point at a mock
            server; defaults to ``https://api.notion.com``.
    """
    client = RESTClient(
        base_url=base_url,
        auth=BearerTokenAuth(integration_token),
        headers={"Notion-Version": NOTION_API_VERSION},
    )

    @dlt.resource(
        name="users",
        primary_key="id",
        write_disposition="replace",
        columns=_USERS_COLUMNS,
    )
    def users() -> Iterator[Row]:
        """Workspace members — full replace each sync.

        Notion workspaces typically have a small, slowly-changing member set so
        a full replace is cheaper than tracking individual changes.
        """
        for page in client.paginate(
            PATH_USERS,
            paginator=users_paginator(),
            data_selector="results",
        ):
            yield from page

    @dlt.resource(
        name="databases",
        primary_key="id",
        write_disposition="append",
        columns=_DATABASES_COLUMNS,
    )
    def databases(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "last_edited_time",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        """Notion databases visible to the integration, sorted newest-first.

        Uses ``last_edited_time`` as the incremental cursor.  The search
        results are returned **descending** by ``last_edited_time``, so once
        we encounter an item whose timestamp is at or below the prior
        high-water mark we can stop paging — all subsequent items are older.

        ``cursor.start_value`` (not ``cursor.last_value``) is the stable
        high-water mark from the *previous* run.  ``last_value`` shifts during
        the current run and would cause items ingested earlier in the same run
        to be skipped on a restart.
        """
        threshold = cursor.start_value
        stop = False
        for page in client.paginate(
            PATH_SEARCH,
            method="POST",
            json={
                "filter": {"value": "database", "property": "object"},
                "sort": {
                    "direction": "descending",
                    "timestamp": "last_edited_time",
                },
            },
            paginator=search_paginator(),
            data_selector="results",
        ):
            for item in page:
                if item.get("last_edited_time", "") <= threshold:
                    stop = True
                    break
                yield item
            if stop:
                break

    @dlt.resource(
        name="pages",
        primary_key="id",
        write_disposition="append",
        columns=_PAGES_COLUMNS,
    )
    def pages(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "last_edited_time",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        """Notion pages visible to the integration, sorted newest-first.

        Same incremental + early-termination strategy as ``databases``.

        Each row also carries a ``page_id`` alias (equal to ``id``) so the
        ``blocks`` transformer can reference the originating page without
        relying on dlt's internal ``_dlt_parent_id`` mechanism.
        """
        threshold = cursor.start_value
        stop = False
        for page in client.paginate(
            PATH_SEARCH,
            method="POST",
            json={
                "filter": {"value": "page", "property": "object"},
                "sort": {
                    "direction": "descending",
                    "timestamp": "last_edited_time",
                },
            },
            paginator=search_paginator(),
            data_selector="results",
        ):
            for item in page:
                if item.get("last_edited_time", "") <= threshold:
                    stop = True
                    break
                # `page_id` alias lets the blocks transformer join without
                # dlt internals; mirrors Airbyte's source-notion output shape.
                yield {**item, "page_id": item["id"]}
            if stop:
                break

    @dlt.transformer(
        data_from=pages,
        name="blocks",
        primary_key="id",
        write_disposition="append",
        columns=_BLOCKS_COLUMNS,
    )
    def blocks(
        page: Row,
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "last_edited_time",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        """Top-level block children for each ingested page.

        Notion's integration token must be **explicitly shared** with each
        page in the Notion UI (Share → Add integration).  Pages that have
        not been shared return HTTP 404; those are skipped with a WARNING log
        rather than failing the whole run.  Any other HTTP error is re-raised.

        Block filtering uses ``last_edited_time`` to skip blocks not updated
        since the prior run.  Unlike the search resources, the blocks endpoint
        is not sorted, so we *skip* rather than break-early on old items.
        """
        threshold = cursor.start_value
        page_id = page["id"]
        try:
            for batch in client.paginate(
                PATH_BLOCKS_CHILDREN.format(page_id=page_id),
                paginator=children_paginator(),
                data_selector="results",
            ):
                for block in batch:
                    if block.get("last_edited_time", "") <= threshold:
                        # Block children are not sorted by edit time —
                        # skip stale blocks rather than breaking out of
                        # the loop early.
                        continue
                    yield {**block, "page_id": page_id}
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == HTTP_NOT_FOUND:
                logger.warning(
                    "Notion integration not shared with page %s; skipping blocks. "
                    "Share the integration with this page in the Notion UI "
                    "(Share → Add integration) to start ingesting its blocks.",
                    page_id,
                )
                return
            raise

    @dlt.transformer(
        data_from=blocks,
        name="comments",
        primary_key="id",
        write_disposition="append",
        columns=_COMMENTS_COLUMNS,
    )
    def comments(block: Row) -> Iterator[Row]:
        """Comments attached to each block.

        Comments do **not** expose a monotonic update timestamp, so this
        resource uses ``append`` + dedup-on-id at the staging layer rather
        than an incremental cursor.

        404 handling mirrors ``blocks``: an un-shared parent page causes 404
        on the comments endpoint too; those are silently skipped.

        HTTP 400 ``unauthorized_capability`` means the integration was minted
        without the ``read_comment`` capability — that is a configuration
        error worth surfacing, so non-404 errors are always re-raised.
        """
        block_id = block["id"]
        try:
            for batch in client.paginate(
                PATH_COMMENTS,
                params={"block_id": block_id},
                paginator=comments_paginator(),
                data_selector="results",
            ):
                for comment in batch:
                    yield {**comment, "block_id": block_id}
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == HTTP_NOT_FOUND:
                return
            raise

    return [users, databases, pages, blocks, comments]


__all__ = ["notion_source"]
