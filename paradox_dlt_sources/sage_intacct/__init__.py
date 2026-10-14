"""Sage Intacct (REST API v1) dlt source — GL detail, AR, AP and dimensions.

Reads Sage Intacct objects through the query service
(``POST /services/core/query``) with an explicit field list per object (see
``settings.OBJECTS``).  ``journal_entry_lines`` is the GL-detail table that
feeds P&L / gross-margin / EBITDA reporting; the remaining resources are its
headers, AR / AP sub-ledgers, budgets and dimension tables.

Multi-entity
------------
``entity_ids`` lists the entities to read.  Each entity gets its own OAuth
token scoped with the ``user@company|entity`` username, every row is tagged
with ``entity_id``, and primary keys are ``(entity_id, key)``.  An empty list
reads the top level once and tags rows with ``entity_id = ""``.

Incremental
-----------
Objects that expose ``audit.modifiedDateTime`` (everything except the small
config objects entities / locations / departments / classes, which are fully
replaced each run) are merged on ``(entity_id, key)`` and filtered with
``audit.modifiedDateTime >= cursor.start_value``.  ``start_value`` is the
stable high-water mark from the *previous* run — **not** ``cursor.last_value``,
which shifts mid-extract and would drop rows across entities or pages (the
repo-wide convention).

Auth
----
OAuth 2.0 client credentials against ``/oauth2/token``.  Tokens are cached per
entity scope and refreshed shortly before ``expires_in`` elapses, or after a
401.  Configure ``.dlt/secrets.toml`` → ``[sources.sage_intacct]`` with
``client_id`` / ``client_secret`` / ``company_id`` / ``user_id`` and optional
``entity_ids``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import dlt
import requests
from dlt.sources import DltResource

from .helpers import (
    SageIntacctAuth,
    build_username,
    columns_for,
    entity_scopes,
    query_pages,
    to_row,
)
from .settings import (
    API_BASE_URL,
    CURSOR_COLUMN,
    DEFAULT_INITIAL_MODIFIED_SINCE,
    DEFAULT_PAGE_SIZE,
    OBJECTS,
    PRIMARY_KEY,
    TOP_LEVEL_ENTITY_ID,
    SageObject,
)

__all__ = ["sage_intacct_source"]


@dlt.source(name="sage_intacct", max_table_nesting=0)
def sage_intacct_source(
    company_id: str = dlt.config.value,
    user_id: str = dlt.config.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    entity_ids: list[str] | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    initial_modified_since: str = DEFAULT_INITIAL_MODIFIED_SINCE,
    base_url: str = API_BASE_URL,
) -> Sequence[DltResource]:
    """Build the Sage Intacct source.

    Args:
        company_id: Sage Intacct company ID, e.g. ``interstateac``.
        user_id: Web Services user ID authorised for the client application.
        client_id: Client ID of the Sage developer-console application.
        client_secret: Client secret of that application.
        entity_ids: Entity IDs to read; empty or ``None`` reads the top level.
        page_size: Records per query request (max 1000).
        initial_modified_since: ``audit.modifiedDateTime`` lower bound for the
            first incremental run (ISO 8601).
        base_url: REST API base URL.

    Returns:
        One resource per entry in ``settings.OBJECTS``.
    """
    session = requests.Session()
    scopes = [
        (
            entity_id,
            SageIntacctAuth(
                client_id=client_id,
                client_secret=client_secret,
                username=build_username(user_id, company_id, entity_id),
                base_url=base_url,
                session=session,
            ),
        )
        for entity_id in entity_scopes(entity_ids, TOP_LEVEL_ENTITY_ID)
    ]
    return [
        _object_resource(obj, session, scopes, page_size, initial_modified_since) for obj in OBJECTS
    ]


def _extract(
    obj: SageObject,
    session: requests.Session,
    scopes: Sequence[tuple[str, SageIntacctAuth]],
    page_size: int,
    modified_since: str | None,
) -> Iterator[Any]:
    emitted = False
    for entity_id, auth in scopes:
        for page in query_pages(session, auth, obj, page_size, modified_since):
            emitted = True
            yield [to_row(record, entity_id) for record in page]
    if not emitted:
        # Creates the table from the column hints even when nothing was read.
        yield dlt.mark.materialize_table_schema()


def _object_resource(
    obj: SageObject,
    session: requests.Session,
    scopes: Sequence[tuple[str, SageIntacctAuth]],
    page_size: int,
    initial_modified_since: str,
) -> DltResource:
    columns = columns_for(obj)
    if obj.incremental:

        def incremental_rows(
            cursor: Any = dlt.sources.incremental(  # noqa: B008
                CURSOR_COLUMN,
                initial_value=initial_modified_since,
                range_start="closed",
            ),
        ) -> Iterator[Any]:
            yield from _extract(obj, session, scopes, page_size, cursor.start_value)

        return dlt.resource(
            incremental_rows,
            name=obj.resource,
            primary_key=list(PRIMARY_KEY),
            write_disposition="merge",
            columns=columns,
        )

    def full_rows() -> Iterator[Any]:
        yield from _extract(obj, session, scopes, page_size, None)

    return dlt.resource(
        full_rows,
        name=obj.resource,
        primary_key=list(PRIMARY_KEY),
        write_disposition="replace",
        columns=columns,
    )
