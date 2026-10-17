"""Ellucian Ethos dlt source — Banner data through Ethos Integration.

Resources
---------
One upsert resource per Ethos resource (``settings.RESOURCES``), keyed by the
Ethos GUID ``id`` and named after the resource in snake_case
(``section-registrations`` → ``section_registrations``).  Nested objects are
kept as ``json`` (``max_table_nesting=0``).  With the change feed on, each
also gets an insert-only ``<table>_versions`` resource, and every message
lands in ``change_notifications``.

Change capture
--------------
Ethos resources have no modification-time filter, so the change mechanism is
the change feed (CONNECTOR_SPEC §11.1 option 1): the change notifications
Banner publishes for the resources the Ethos application subscribes to.

* First run of a resource (and ``resync=True``): full ``offset`` / ``limit``
  read.  Subscriptions must exist before that run, so changes made during the
  read stay queued (§11.3).
* Later runs: one ``/consume`` batch per run, routed to each resource.  The
  latest message per id is upserted; ``deleted`` operations become tombstones.
* Every ``delete_reconciliation_interval``: a full read again, whose ids are
  the live set (§12.2 method 4).  This also covers resources nobody subscribed to.

With ``change_feed=False`` every run is a full read plus key reconciliation
(§11.1 option 3).
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    ChangeFeed,
    FullRead,
    Row,
    change_position,
    check_reconciliation,
    columns,
    make_client,
    media_type,
    parse_timestamp,
    read_stored_rows,
    tombstones,
    transform_row,
)
from .settings import (
    API_PATH,
    CHANGE_NOTIFICATIONS,
    DEFAULT_BASE_URL,
    DEFAULT_FEED_BATCH_SIZE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RECONCILIATION_INTERVAL_HOURS,
    RESOURCES,
    RESOURCES_BY_NAME,
    VERSIONS_SUFFIX,
    EthosResource,
)

__all__ = ["ellucian_ethos_source"]

logger = logging.getLogger(__name__)

_SNAPSHOT_STATE_KEY = "snapshot_at"
_RECONCILED_STATE_KEY = "reconciled_at"
_DELETE = "deleted"
_NOT_FOUND = 404
_CHANGE_OPS: Mapping[str, str] = {"created": "insert", "replaced": "update", "updated": "update"}


@dlt.source(name="ellucian_ethos", max_table_nesting=0)
def ellucian_ethos_source(
    credentials: dict[str, Any] = dlt.secrets.value,
    base_url: str = DEFAULT_BASE_URL,
    resources: list[str] | None = None,
    extra_resources: list[str] | None = None,
    versions: dict[str, str] | None = None,
    change_feed: bool = True,
    resync: bool = False,
    start_date: str | None = None,
    lookback: dt.timedelta | None = None,
    include_history: bool = True,
    include_versions: bool = True,
    delete_reconciliation_interval: dt.timedelta | None = dt.timedelta(  # noqa: B008
        hours=DEFAULT_RECONCILIATION_INTERVAL_HOURS
    ),
    page_size: int = DEFAULT_PAGE_SIZE,
    feed_batch_size: int = DEFAULT_FEED_BATCH_SIZE,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> list[DltResource]:
    """Load Banner resources from Ellucian Ethos Integration.

    Args:
        credentials: ``api_key`` of the Ethos application this connector runs as.
        base_url: Integration Management root; change it for other regions
            (``https://integrate.elluciancloud.ca``, ``.ie``, ``.com.au``).
        resources: Ethos resource names to load.  ``None`` loads every
            unrestricted resource in ``settings.RESOURCES``; restricted ones
            (FAFSA applications) load only when named.
        extra_resources: More Ethos resources to load, such as RMU custom APIs.
        versions: Pinned version per resource (``{"persons": "12"}`` or
            ``"16.0.0"``).  Unpinned resources get Ethos' latest version.
        change_feed: Read change notifications.  Needs subscriptions on the
            Ethos application.  ``False`` makes every run a full read.
        resync: Full read of every selected resource this run (§11.3).
        start_date: Not applicable; Ethos has no modification-time filter.
            Accepted for the standard interface (§14) and ignored.
        lookback: Not applicable for the same reason; ignored.
        include_history: Not applicable; Ethos exposes no field history (§13.1).
        include_versions: Emit ``<table>_versions`` from the change feed (§13.2).
        delete_reconciliation_interval: How often a feed-driven resource is read
            in full to find deletes.  ``None`` relies on the feed's ``deleted``
            operations alone.
        page_size: Rows requested per call.  A smaller ``X-Max-Page-Size``
            from the server wins.
        feed_batch_size: Messages read from ``/consume`` per run.
        max_attempts: Attempts per request on 429 / 5xx / connection errors.

    Returns:
        One resource per Ethos resource, plus ``<table>_versions`` and
        ``change_notifications`` when the change feed is on.

    Raises:
        ValueError: On unknown resource names or a missing API key.
    """
    if start_date or lookback:
        logger.warning("ellucian_ethos: start_date and lookback do not apply to Ethos; ignored")
    del include_history
    selected = _select_resources(resources, extra_resources)
    client = make_client(base_url, str(credentials.get("api_key") or ""), max_attempts)
    feed = ChangeFeed(client, feed_batch_size) if change_feed else None
    pins = versions or {}

    out: list[DltResource] = []
    for resource in selected:
        out.append(
            _entity_resource(
                client,
                resource,
                pins.get(resource.name),
                feed,
                resync=resync,
                reconciliation_interval=delete_reconciliation_interval,
                page_size=page_size,
            )
        )
        if feed is not None and include_versions:
            out.append(_versions_resource(client, resource, feed))
    if feed is not None:
        out.append(_change_notifications_resource(feed))
    return out


def _select_resources(
    resources: Sequence[str] | None, extra: Sequence[str] | None
) -> list[EthosResource]:
    """Resolve ``resources`` and ``extra_resources`` to the resources to load."""
    extras = [EthosResource(n) for n in (extra or []) if n not in RESOURCES_BY_NAME]
    if resources is None:
        return [r for r in RESOURCES if not r.restricted] + extras
    known = {*RESOURCES_BY_NAME, *(e.name for e in extras)}
    unknown = sorted(set(resources) - known)
    if unknown:
        raise ValueError(
            f"ellucian_ethos: unknown resource(s) {unknown}; add them to extra_resources "
            f"or pick from {sorted(RESOURCES_BY_NAME)}"
        )
    wanted = set(resources)
    return [r for r in (*RESOURCES, *extras) if r.name in wanted]


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _due(state: Mapping[str, Any], interval: dt.timedelta | None) -> bool:
    if interval is None:
        return False
    last = state.get(_RECONCILED_STATE_KEY)
    return last is None or _now() - dt.datetime.fromisoformat(last) >= interval


def _resolve_content(
    client: RESTClient, feed: ChangeFeed, resource: EthosResource, message: Row
) -> Row | None:
    """The full record a message refers to, fetched by id when sent without content.

    A subscription can be set to publish notifications without content; the
    record is then read with ``GET /api/{resource}/{id}`` in the message's
    version.  A 404 means it was deleted since, and returns ``None``.
    """
    content = message.get("content")
    if isinstance(content, Mapping) and content:
        return dict(content)
    key = str(message["id"])
    if key not in feed.fetched:
        ref = message.get("resource") or {}
        response = client.get(
            f"{API_PATH.format(resource=resource.name)}/{ref['id']}",
            headers={"Accept": ref.get("version") or media_type(None)},
        )
        if response.status_code == _NOT_FOUND:
            feed.fetched[key] = None
        else:
            response.raise_for_status()
            feed.fetched[key] = response.json()
    return feed.fetched[key]


def _feed_rows(
    client: RESTClient, feed: ChangeFeed, resource: EthosResource, extracted_at: dt.datetime
) -> Iterator[tuple[Row, Row]]:
    """Yield (message, row) per message for one resource; deletes as tombstone rows."""
    for message in feed.for_resource(resource.name):
        ref = message.get("resource") or {}
        published = parse_timestamp(message.get("published"))
        content = (
            None
            if message.get("operation") == _DELETE
            else _resolve_content(client, feed, resource, message)
        )
        if content is None:
            row: Row = {
                "id": str(ref["id"]),
                "_source_created_at": None,
                "_source_updated_at": published,
                "_is_deleted": True,
                "_deleted_at": published or extracted_at,
                "_extracted_at": extracted_at,
                "_ethos_media_type": ref.get("version"),
            }
        else:
            row = transform_row(content, ref.get("version"), extracted_at)
            row["id"] = str(row.get("id") or ref["id"])
            row["_source_updated_at"] = row["_source_updated_at"] or published
        yield message, row


def _entity_resource(
    client: RESTClient,
    resource: EthosResource,
    version: str | None,
    feed: ChangeFeed | None,
    *,
    resync: bool,
    reconciliation_interval: dt.timedelta | None,
    page_size: int,
) -> DltResource:
    """Build the upsert resource for one Ethos resource."""
    label = resource.name

    def rows() -> Iterator[list[Row]]:
        state = dlt.current.resource_state()
        extracted_at = _now()
        full = (
            feed is None
            or resync
            or state.get(_SNAPSHOT_STATE_KEY) is None
            or _due(state, reconciliation_interval)
        )
        if full:
            yield from _full_read(state, extracted_at)
        if feed is not None and not full:
            # A full read this run already holds the current state; replaying
            # older queued messages over it would load stale rows (and put one
            # key twice in an upsert batch).  The messages still land in the
            # versions table and change_notifications.
            yield from _apply_feed(extracted_at)

    def _full_read(state: dict[str, Any], extracted_at: dt.datetime) -> Iterator[list[Row]]:
        read = FullRead(client, resource.name, version, page_size)
        seen: set[str] = set()
        for page in read.pages():
            media = read.paginator.media_type or media_type(version)
            out = [transform_row(r, media, extracted_at) for r in page]
            fresh = []
            for row in out:
                row["id"] = str(row["id"])
                if row["id"] not in seen:
                    seen.add(row["id"])
                    fresh.append(row)
            yield fresh
        logger.info("ellucian_ethos: %s: %d rows extracted", label, len(seen))
        if read.paginator.content_restricted:
            logger.warning(
                "ellucian_ethos: %s: Banner removed fields (X-Content-Restricted); "
                "check GORDMSK rules for the Ethos API user",
                label,
            )
        if not read.complete:
            # Offset paging over changing data can skip rows; deleting on an
            # incomplete walk would tombstone live records.
            logger.warning(
                "ellucian_ethos: %s: read %d of %s rows; skipping delete reconciliation",
                label,
                read.rows_read,
                read.paginator.total,
            )
        else:
            stored = read_stored_rows(resource.table)
            check_reconciliation(stored, seen, label=label)
            deleted = tombstones(stored, seen, extracted_at)
            if deleted:
                logger.info("ellucian_ethos: %s: %d rows marked deleted", label, len(deleted))
                yield deleted
            state[_RECONCILED_STATE_KEY] = extracted_at.isoformat()
        state.setdefault(_SNAPSHOT_STATE_KEY, extracted_at.isoformat())

    def _apply_feed(extracted_at: dt.datetime) -> Iterator[list[Row]]:
        assert feed is not None
        latest: dict[str, Row] = {}
        for _, row in _feed_rows(client, feed, resource, extracted_at):
            latest[row["id"]] = row
        if not latest:
            return
        deleted_ids = {i for i, r in latest.items() if r["_is_deleted"]}
        stored = {str(r["id"]): r for r in read_stored_rows(resource.table, deleted_ids)}
        out = [
            # Keep the stored data on the tombstone (§12.1).
            {**stored[row_id], **{k: v for k, v in row.items() if k.startswith("_")}}
            if row_id in stored
            else row
            for row_id, row in latest.items()
        ]
        logger.info(
            "ellucian_ethos: %s: %d changes applied (%d deletes)",
            label,
            len(out),
            len(deleted_ids),
        )
        yield out

    return dlt.resource(
        rows,
        name=resource.table,
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        columns=columns(),
    )


def _versions_resource(
    client: RESTClient, resource: EthosResource, feed: ChangeFeed
) -> DltResource:
    """Build ``<table>_versions``: one row per published change (§13.2)."""

    def rows() -> Iterator[list[Row]]:
        extracted_at = _now()
        out = []
        for message, row in _feed_rows(client, feed, resource, extracted_at):
            operation = str(message.get("operation") or "")
            out.append(
                {
                    **row,
                    "_change_op": "delete" if row["_is_deleted"] else _CHANGE_OPS.get(operation),
                    "_change_position": change_position(message["id"]),
                    "_committed_at": parse_timestamp(message.get("published")),
                }
            )
        if out:
            yield out

    return dlt.resource(
        rows,
        name=resource.table + VERSIONS_SUFFIX,
        primary_key=["id", "_change_position"],
        write_disposition={"disposition": "merge", "strategy": "insert-only"},
        columns=columns(change=True),
    )


def _change_notifications_resource(feed: ChangeFeed) -> DltResource:
    """Build ``change_notifications``: every consumed message, as received.

    Once consumed, a message is gone from Ethos, so this table is the replay
    log, including messages for resources this run did not load.
    """

    def rows() -> Iterator[list[Row]]:
        extracted_at = _now()
        out = [
            {
                **message,
                "id": str(message["id"]),
                "_change_position": change_position(message["id"]),
                "_committed_at": parse_timestamp(message.get("published")),
                "_extracted_at": extracted_at,
            }
            for message in feed.messages()
        ]
        if out:
            yield out

    return dlt.resource(
        rows,
        name=CHANGE_NOTIFICATIONS,
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "insert-only"},
        columns={
            "id": {"data_type": "text", "nullable": False},
            "_change_position": {"data_type": "text", "nullable": False},
            "_committed_at": {"data_type": "timestamp", "nullable": True},
            "_extracted_at": {"data_type": "timestamp", "nullable": False},
            "resource": {"data_type": "json", "nullable": True},
            "content": {"data_type": "json", "nullable": True},
        },
    )
