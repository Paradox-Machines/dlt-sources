"""Ellucian Ethos source — auth, offset paging, change feed and row transforms."""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

import dlt
import requests
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import PreparedRequest, Request, Response
from requests.auth import AuthBase

from .settings import (
    API_PATH,
    AUTH_PATH,
    CHANGE_COLUMNS,
    CHANGE_NOTIFICATIONS_MEDIA_TYPE,
    CONSUME_PATH,
    CONTENT_RESTRICTED_HEADER,
    LATEST_MEDIA_TYPE,
    MAX_PAGE_SIZE_HEADER,
    MAX_RETRY_DELAY_SECONDS,
    MEDIA_TYPE_HEADER,
    MEDIA_TYPE_TEMPLATE,
    REMAINING_HEADER,
    REQUEST_TIMEOUT_SECONDS,
    STANDARD_COLUMNS,
    TOKEN_REFRESH_SAFETY_SECONDS,
    TOKEN_TTL_SECONDS,
    TOTAL_COUNT_HEADER,
    USER_AGENT,
)

LOGGER = logging.getLogger(__name__)

Row = dict[str, Any]

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class EthosTokenAuth(AuthBase):
    """Bearer auth with an Integration Management access token.

    The API key is exchanged at ``POST /auth`` for a JWT that expires after
    five minutes, so a backfill refreshes it many times.  The token is cached
    until ``TOKEN_REFRESH_SAFETY_SECONDS`` before expiry.

    Args:
        base_url: Integration Management root (``https://integrate.elluciancloud.com``).
        api_key: API key of the Ethos application this connector runs as.
    """

    def __init__(self, base_url: str, api_key: str) -> None:
        self._auth_url = base_url.rstrip("/") + AUTH_PATH
        self._api_key = api_key
        self._token: str | None = None
        self._expires_at = 0.0

    def __repr__(self) -> str:
        """Return a repr that never leaks the API key."""
        return f"EthosTokenAuth(auth_url={self._auth_url!r})"

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Attach the bearer token, refreshing it if stale."""
        request.headers["Authorization"] = f"Bearer {self.token()}"
        request.headers["User-Agent"] = USER_AGENT
        return request

    def token(self) -> str:
        """Return a valid access token, fetching a new one when needed."""
        if self._token is None or time.time() >= self._expires_at - TOKEN_REFRESH_SAFETY_SECONDS:
            response = requests.post(
                self._auth_url,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Accept": "text/plain",
                    "User-Agent": USER_AGENT,
                },
                timeout=30,
            )
            response.raise_for_status()
            self._token = response.text.strip().strip('"')
            self._expires_at = time.time() + TOKEN_TTL_SECONDS
        return self._token


def make_client(base_url: str, api_key: str, max_attempts: int) -> RESTClient:
    """Return a ``RESTClient`` on Integration Management.

    The session retries 429, 5xx and dropped connections with backoff and
    honours ``Retry-After``.

    Args:
        base_url: Integration Management root.
        api_key: Ethos application API key.
        max_attempts: Attempts per request.

    Raises:
        ValueError: When the API key is empty.
    """
    if not api_key:
        raise ValueError("ellucian_ethos: credentials.api_key is required")
    session = Client(
        request_timeout=REQUEST_TIMEOUT_SECONDS,
        raise_for_status=False,
        request_max_attempts=max_attempts,
        request_max_retry_delay=MAX_RETRY_DELAY_SECONDS,
        respect_retry_after_header=True,
    ).session
    return RESTClient(
        base_url=base_url.rstrip("/"),
        auth=EthosTokenAuth(base_url, api_key),
        session=session,
    )


def media_type(version: str | None) -> str:
    """``Accept`` value for a pinned major/semantic version, or latest."""
    return MEDIA_TYPE_TEMPLATE.format(version=version) if version else LATEST_MEDIA_TYPE


# ---------------------------------------------------------------------------
# Offset paging
# ---------------------------------------------------------------------------


class EthosOffsetPaginator(BasePaginator):
    """``offset`` / ``limit`` paging driven by ``X-Total-Count``.

    The offset advances by the rows actually returned, so a server-side page
    cap smaller than ``limit`` (``X-Max-Page-Size``) does not skip rows.  The
    walk ends when the offset reaches the first total seen, or on an empty page.

    Args:
        limit: Rows requested per call.
    """

    def __init__(self, limit: int) -> None:
        super().__init__()
        if limit <= 0:
            raise ValueError("ellucian_ethos: page_size must be positive")
        self.limit = limit
        self.offset = 0
        self.total: int | None = None
        self.media_type: str | None = None
        self.content_restricted = False

    def init_request(self, request: Request) -> None:
        """Seed ``offset`` and ``limit``."""
        if request.params is None:
            request.params = {}
        request.params["offset"] = self.offset
        request.params["limit"] = self.limit

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        """Advance the offset; read the total, page cap and media type."""
        rows = data or []
        header_total = response.headers.get(TOTAL_COUNT_HEADER)
        if self.total is None and header_total is not None:
            self.total = int(header_total)
        cap = response.headers.get(MAX_PAGE_SIZE_HEADER)
        if cap is not None and int(cap) < self.limit:
            self.limit = int(cap)
        self.media_type = response.headers.get(MEDIA_TYPE_HEADER) or self.media_type
        if response.headers.get(CONTENT_RESTRICTED_HEADER):
            self.content_restricted = True
        self.offset += len(rows)
        if not rows:
            self._has_next_page = False
        elif self.total is not None:
            self._has_next_page = self.offset < self.total
        else:
            self._has_next_page = True

    def update_request(self, request: Request) -> None:
        """Ask for the next window."""
        if request.params is None:
            request.params = {}
        request.params["offset"] = self.offset
        request.params["limit"] = self.limit


class FullRead:
    """Result of walking one resource: rows per page, then the walk's totals.

    Args:
        client: Integration Management client.
        resource: Ethos resource name.
        version: Pinned version, or ``None`` for latest.
        page_size: Rows requested per call.
    """

    def __init__(
        self, client: RESTClient, resource: str, version: str | None, page_size: int
    ) -> None:
        self._client = client
        self._resource = resource
        self._version = version
        self.paginator = EthosOffsetPaginator(page_size)
        self.rows_read = 0

    def pages(self) -> Iterator[list[Row]]:
        """Yield each page of raw records."""
        for page in self._client.paginate(
            API_PATH.format(resource=self._resource),
            headers={"Accept": media_type(self._version)},
            paginator=self.paginator,
        ):
            rows = list(page)
            self.rows_read += len(rows)
            yield rows

    @property
    def complete(self) -> bool:
        """True when the walk read at least as many rows as the first total."""
        total = self.paginator.total
        return total is None or self.rows_read >= total


# ---------------------------------------------------------------------------
# Change feed
# ---------------------------------------------------------------------------


class ChangeFeed:
    """One ``/consume`` read per run, shared by every resource.

    The read passes the ``lastProcessedID`` saved by the last *successful*
    run, which acknowledges only messages already loaded.  The new position is
    written to source state, and dlt commits state only with a successful
    load, so a failed run re-reads the same messages next time (CONNECTOR_SPEC
    §11.2).  Exactly one batch is read per run for that reason: reading a second
    batch would acknowledge the first before it is loaded.

    Args:
        client: Integration Management client.
        batch_size: ``limit`` sent to ``/consume``.
    """

    STATE_KEY = "last_processed_id"

    def __init__(self, client: RESTClient, batch_size: int) -> None:
        self._client = client
        self._batch_size = batch_size
        self._messages: list[Row] | None = None
        self.remaining: int | None = None
        # Records fetched for messages sent without content, by message id,
        # so the entity and versions resources fetch each one once.
        self.fetched: dict[str, Row | None] = {}

    def messages(self) -> list[Row]:
        """Fetch the batch on first use; later calls return the same batch."""
        if self._messages is not None:
            return self._messages
        state = dlt.current.source_state()
        params: dict[str, Any] = {"limit": self._batch_size}
        last = state.get(self.STATE_KEY)
        if last is not None:
            params["lastProcessedID"] = last
        response = self._client.get(
            CONSUME_PATH,
            params=params,
            headers={"Accept": CHANGE_NOTIFICATIONS_MEDIA_TYPE},
        )
        response.raise_for_status()
        body = response.json() if response.content else []
        messages = sorted(body or [], key=lambda m: int(m["id"]))
        remaining = response.headers.get(REMAINING_HEADER)
        self.remaining = int(remaining) if remaining is not None else None
        if messages:
            state[self.STATE_KEY] = int(messages[-1]["id"])
        if self.remaining:
            LOGGER.warning(
                "ellucian_ethos: %d change notifications still queued after this batch; "
                "run again (or raise feed_batch_size) to catch up",
                self.remaining,
            )
        self._messages = messages
        return messages

    def for_resource(self, resource: str) -> list[Row]:
        """Messages about one Ethos resource, in publish order."""
        return [m for m in self.messages() if (m.get("resource") or {}).get("name") == resource]


def change_position(message_id: Any) -> str:
    """Encode a message id so it sorts correctly as text."""
    return f"{int(message_id):020d}"


# ---------------------------------------------------------------------------
# Row transforms and schema hints
# ---------------------------------------------------------------------------


def parse_timestamp(value: Any) -> dt.datetime | None:
    """Parse an Ethos ISO 8601 timestamp to aware UTC; ``None`` when absent.

    Ethos datetimes carry an offset or ``Z``.  A value without one is read as
    UTC, the timezone Ethos documents for its datetimes.
    """
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def transform_row(record: Mapping[str, Any], media: str | None, extracted_at: dt.datetime) -> Row:
    """Add the standard columns to one Ethos record.

    ``_source_created_at`` / ``_source_updated_at`` come from the optional
    EEDM ``metadata.createdOn`` / ``metadata.modifiedOn`` when Banner sends them.

    Args:
        record: Raw Ethos representation.
        media: Media type the record was served as.
        extracted_at: Run timestamp.
    """
    raw_metadata = record.get("metadata")
    metadata: Mapping[str, Any] = raw_metadata if isinstance(raw_metadata, Mapping) else {}
    out: Row = dict(record)
    out["_source_created_at"] = parse_timestamp(metadata.get("createdOn"))
    out["_source_updated_at"] = parse_timestamp(metadata.get("modifiedOn"))
    out["_is_deleted"] = False
    out["_deleted_at"] = None
    out["_extracted_at"] = extracted_at
    out["_ethos_media_type"] = media
    return out


def columns(*, change: bool = False) -> dict[str, dict[str, Any]]:
    """Column hints for an Ethos table: ``id`` plus the standard columns.

    Each column gets its own dict; dlt mutates hints in place.

    Args:
        change: Also declare the change-feed columns (versions tables).
    """
    names = {**STANDARD_COLUMNS, **(CHANGE_COLUMNS if change else {})}
    hints: dict[str, dict[str, Any]] = {"id": {"data_type": "text", "nullable": False}}
    for name, dtype in names.items():
        hints[name] = {"data_type": dtype, "nullable": name != "_is_deleted"}
    return hints


# ---------------------------------------------------------------------------
# Delete detection (CONNECTOR_SPEC §12.2 method 4: key reconciliation)
# ---------------------------------------------------------------------------

_DLT_COLUMNS = ("_dlt_load_id", "_dlt_id")


def read_stored_rows(table: str, ids: set[str] | None = None) -> list[Row]:
    """Return live (not deleted) rows already in the destination.

    Read through the pipeline's SQL client, so it works on any SQL-queryable
    destination (duckdb, Iceberg via the filesystem SQL client).  Returns
    ``[]`` when the table does not exist yet.

    Args:
        table: Destination table name.
        ids: Keep only these ids (``None`` keeps all).
    """
    pipeline = dlt.current.pipeline()
    schema = dlt.current.source_schema()
    if table not in schema.tables:
        return []
    json_columns = {
        name
        for name, col in schema.get_table_columns(table, include_incomplete=True).items()
        if col.get("data_type") == "json"
    }
    try:
        with pipeline.sql_client() as client:
            qualified = client.make_qualified_table_name(table)
            with client.execute_query(f"SELECT * FROM {qualified}") as cursor:
                names = [d[0] for d in cursor.description]
                fetched = cursor.fetchall()
    except (DatabaseUndefinedRelation, DestinationUndefinedEntity):
        return []
    rows = []
    for values in fetched:
        row = dict(zip(names, values, strict=True))
        if row.get("_is_deleted") or (ids is not None and row.get("id") not in ids):
            continue
        for name in _DLT_COLUMNS:
            row.pop(name, None)
        for name in json_columns:
            if isinstance(row.get(name), str):
                row[name] = json.loads(row[name])
        rows.append(row)
    return rows


def tombstones(stored: Iterable[Row], live_ids: set[str], deleted_at: dt.datetime) -> list[Row]:
    """Re-emit every stored row whose id is no longer live, marked deleted.

    The whole row is re-emitted because an upsert replaces the row: a key-only
    tombstone would wipe its data (§12.1 keeps it).

    Args:
        stored: Live rows already in the destination.
        live_ids: Ids the source still holds.
        deleted_at: When the delete was detected.
    """
    return [
        {**row, "_is_deleted": True, "_deleted_at": deleted_at, "_extracted_at": deleted_at}
        for row in stored
        if str(row.get("id")) not in live_ids
    ]


def check_reconciliation(stored: Sequence[Row], live_ids: set[str], *, label: str) -> None:
    """Refuse to tombstone a whole table from an empty read.

    An empty read against a populated table is far more likely a lost
    permission or an unpublished resource than every record being deleted.

    Raises:
        RuntimeError: When the source returned no ids but rows are loaded.
    """
    if stored and not live_ids:
        raise RuntimeError(
            f"ellucian_ethos: {label}: the source returned no records but {len(stored)} rows are "
            "loaded; refusing to mark them all deleted. Check the Ethos application's access."
        )
