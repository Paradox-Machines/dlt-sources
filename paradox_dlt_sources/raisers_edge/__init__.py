"""Raiser's Edge NXT dlt source, read through the Blackbaud SKY API.

Spec type 1 (§P1), level L2. Entities are read from SKY API list-all endpoints with a
server-side ``last_modified`` cursor; ``sort_token`` paging keeps the order stable.
SKY API lists never report deletes, and its only change feed is webhooks (which need a
public receiver), so deletes come from key reconciliation against the destination.
See README.md for the resource matrix.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Sequence
from datetime import timedelta
from typing import Any

import dlt
import pendulum
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

from .helpers import (
    ChangeWindow,
    SkyOAuth2Auth,
    change_params,
    columns,
    finish_reconciliation,
    flatten_objects,
    iter_rows,
    make_client,
    normalize_row,
    offset_paginator,
    paginator_for,
    raise_on_quota,
    reconciliation_due,
    standard_columns,
    to_utc,
    tombstones,
)
from .settings import (
    API_BASE_URL,
    CODE_TABLE_ENTRIES_PATH,
    CODE_TABLES_PATH,
    CURRENCY_CONFIGURATION_PATH,
    CUSTOM_FIELD_CATEGORIES_PATH,
    CUSTOM_FIELD_CATEGORY_LIMIT,
    CUSTOM_FIELD_RECORD_TYPES,
    DEFAULT_LOOKBACK,
    DEFAULT_MAX_DELETE_SHARE,
    DEFAULT_RECONCILIATION_INTERVAL,
    ENTITY_ENDPOINTS,
    EVENT_LIST_PATH,
    EVENT_PARTICIPANTS_PATH,
    GIFT_BATCHES_PATH,
    GIFT_CHILDREN,
    LIST_TYPES,
    LISTS_PATH,
    PAGE_LIMIT,
    TOKEN_URL,
    ListEndpoint,
)

Row = dict[str, Any]

_UPSERT: TWriteDispositionConfig = {"disposition": "merge", "strategy": "upsert"}
_SKY_DATES = ("date_added", "date_modified")


def _first(row: Row, *names: str) -> Any:
    for name in names:
        if row.get(name) is not None:
            return row[name]
    return None


@dlt.source(name="raisers_edge", max_table_nesting=0)
def raisers_edge_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    subscription_key: str = dlt.secrets.value,
    source_timezone: str = dlt.config.value,
    start_date: str | None = None,
    lookback: timedelta = DEFAULT_LOOKBACK,
    resources: Sequence[str] | None = None,
    delete_reconciliation_interval: timedelta | None = DEFAULT_RECONCILIATION_INTERVAL,
    max_delete_share: float = DEFAULT_MAX_DELETE_SHARE,
    preserve_refresh_token: bool = True,
    on_token_rotation: Callable[[str], None] | None = None,
) -> list[Any]:
    """Raiser's Edge NXT constituents, gifts, fundraising, events and metadata.

    Args:
        client_id: SKY application ID.
        client_secret: SKY application secret.
        refresh_token: Refresh token from the one-time authorization code consent.
        subscription_key: SKY developer subscription key (``Bb-Api-Subscription-Key``).
        source_timezone: IANA zone of the RE environment. Naive SKY date-times (no UTC
            offset) are read in it. Required; never guessed (§8.2).
        start_date: ISO date-time for the first incremental read. Default: everything.
        lookback: Overlap subtracted from the saved cursor on each read (§11.2).
        resources: Subset of resource names to load. Default: all.
        delete_reconciliation_interval: How often entity resources run key
            reconciliation (§12.2 method 4). ``None`` disables delete detection.
        max_delete_share: Largest share of a table's live keys one run may tombstone.
        preserve_refresh_token: Ask SKY to keep the refresh token value on refresh, so
            the stored secret never changes. It then expires 365 days after consent.
        on_token_rotation: Called with each new refresh token when rotation is on.

    Returns:
        The selected dlt resources.
    """
    if on_token_rotation is None:

        def on_token_rotation(_new: str) -> None:
            return None

    auth = SkyOAuth2Auth(
        token_url=os.environ.get("RAISERS_EDGE_TOKEN_URL", TOKEN_URL),
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        subscription_key=subscription_key,
        preserve_refresh_token=preserve_refresh_token,
        on_token_rotation=on_token_rotation,
    )
    client = make_client(os.environ.get("RAISERS_EDGE_API_BASE_URL", API_BASE_URL), auth)
    start = to_utc(start_date, source_timezone) if start_date else None
    currency: dict[str, Any] = {}

    def currency_configuration() -> dict[str, Any]:
        if not currency:
            response = client.get(CURRENCY_CONFIGURATION_PATH, hooks={"response": [raise_on_quota]})
            response.raise_for_status()
            currency.update(response.json() or {})
        return currency

    def currency_code() -> str | None:
        code = currency_configuration().get("currency_code")
        return str(code) if code else None

    all_resources: list[Any] = [
        *(
            _entity_resource(
                ep,
                client,
                source_timezone,
                start,
                lookback,
                delete_reconciliation_interval,
                max_delete_share,
                currency_code,
            )
            for ep in ENTITY_ENDPOINTS
        ),
    ]
    gifts = next(r for r in all_resources if r.name == "gifts")
    all_resources += [_gift_child(gifts, table, field) for table, field in GIFT_CHILDREN]
    all_resources += [
        _event_participants(
            client,
            source_timezone,
            start,
            lookback,
            delete_reconciliation_interval,
            max_delete_share,
        ),
        _gift_batches(client, source_timezone, max_delete_share),
        _lists(client, source_timezone, max_delete_share),
        _field_definitions(client, max_delete_share),
        _field_options(client, max_delete_share),
        _currency_configuration(currency_configuration, max_delete_share),
    ]
    if resources is not None:
        wanted = set(resources)
        unknown = wanted - {r.name for r in all_resources}
        if unknown:
            raise ValueError(f"unknown raisers_edge resources: {sorted(unknown)}")
        all_resources = [r for r in all_resources if r.name in wanted]
    return all_resources


# ---------------------------------------------------------------------------
# Entities: last_modified cursor + key reconciliation
# ---------------------------------------------------------------------------


def _entity_columns(ep: ListEndpoint) -> dict[str, dict[str, Any]]:
    return {
        **standard_columns(),
        **columns(
            text=("id", *ep.text_columns, *(("currency_code",) if ep.money else ())),
            decimal=ep.decimal_columns,
            date_=ep.date_fields,
            timestamp=_SKY_DATES,
        ),
    }


def _entity_resource(
    ep: ListEndpoint,
    client: RESTClient,
    tz: str,
    start: pendulum.DateTime | None,
    lookback: timedelta,
    interval: timedelta | None,
    max_delete_share: float,
    currency_code: Callable[[], str | None],
) -> Any:
    @dlt.resource(
        name=ep.resource,
        primary_key="id",
        write_disposition=_UPSERT,
        columns=_entity_columns(ep),
    )
    def _r() -> Iterator[Row]:
        state = dlt.current.resource_state()
        now = pendulum.now("UTC")
        window = ChangeWindow(state, lookback)
        reconcile = reconciliation_due(state, interval, now)
        floor = None if reconcile else window.floor(start)
        code = currency_code() if ep.money else None
        for raw in iter_rows(
            client,
            ep.path,
            resource=ep.resource,
            params=change_params(ep.params, floor),
            paginator=paginator_for(ep.paging),
        ):
            row = normalize_row(
                raw, tz=tz, date_fields=ep.date_fields, extracted_at=now, currency_code=code
            )
            if window.admit((row["id"],), row["_source_updated_at"]):
                yield row
        if reconcile:
            yield from tombstones(
                ep.resource, ("id",), window.seen, now, max_delete_share=max_delete_share
            )
        if reconcile or (interval is not None and "last_reconciled_at" not in state):
            finish_reconciliation(state, now)
        window.commit()

    return _r


def _gift_child(gifts: Any, table: str, field: str) -> Any:
    """Gift splits / soft credits as their own table, under the §7.4 ``_parent_version`` rule."""

    @dlt.transformer(
        data_from=gifts,
        name=table,
        primary_key=("gift_id", "id"),
        columns={
            **standard_columns(),
            **columns(
                text=(
                    "gift_id",
                    "id",
                    "_parent_version",
                    "currency_code",
                    "fund_id",
                    "campaign_id",
                    "appeal_id",
                    "package_id",
                    "constituent_id",
                ),
                decimal=("amount__value",),
            ),
        },
    )
    def _t(gift: Row) -> Iterator[Row]:
        if gift.get("_is_deleted"):
            return
        updated = gift.get("_source_updated_at")
        version = updated.isoformat() if updated is not None else gift["_extracted_at"].isoformat()
        for child in gift.get(field) or []:
            yield {
                **flatten_objects(child),
                "gift_id": gift["id"],
                "_parent_version": version,
                "_source_created_at": gift.get("_source_created_at"),
                "_source_updated_at": updated,
                "_is_deleted": False,
                "_deleted_at": None,
                "_extracted_at": gift["_extracted_at"],
                **({"currency_code": gift["currency_code"]} if "currency_code" in gift else {}),
            }

    # dlt.transformer's signature only types string dispositions.
    _t.apply_hints(write_disposition=_UPSERT)
    return _t


def _event_participants(
    client: RESTClient,
    tz: str,
    start: pendulum.DateTime | None,
    lookback: timedelta,
    interval: timedelta | None,
    max_delete_share: float,
) -> Any:
    """Participants exist only per event; every run walks every event (README: quota)."""

    @dlt.resource(
        name="event_participants",
        primary_key="id",
        write_disposition=_UPSERT,
        columns={
            **standard_columns(),
            **columns(
                text=(
                    "id",
                    "event_id",
                    "constituent_id",
                    "contact_id",
                    "lookup_id",
                    "name",
                    "rsvp_status",
                    "invitation_status",
                ),
                boolean=("attended", "is_constituent"),
                timestamp=_SKY_DATES,
            ),
        },
    )
    def _r() -> Iterator[Row]:
        state = dlt.current.resource_state()
        now = pendulum.now("UTC")
        window = ChangeWindow(state, lookback)
        reconcile = reconciliation_due(state, interval, now)
        floor = None if reconcile else window.floor(start)
        event_ids = [
            row["id"]
            for row in iter_rows(
                client,
                EVENT_LIST_PATH,
                resource="events",
                params={"include_inactive": "true", "fields": "id", "limit": PAGE_LIMIT},
                paginator=offset_paginator(),
            )
        ]
        for event_id in event_ids:
            for raw in iter_rows(
                client,
                EVENT_PARTICIPANTS_PATH.format(event_id=event_id),
                resource="event_participants",
                params=change_params({}, floor),
                paginator=offset_paginator(),
            ):
                row = normalize_row(raw, tz=tz, date_fields=(), extracted_at=now)
                row["event_id"] = event_id
                if window.admit((row["id"],), row["_source_updated_at"]):
                    yield row
        if reconcile:
            yield from tombstones(
                "event_participants", ("id",), window.seen, now, max_delete_share=max_delete_share
            )
        if reconcile or (interval is not None and "last_reconciled_at" not in state):
            finish_reconciliation(state, now)
        window.commit()

    return _r


# ---------------------------------------------------------------------------
# Full-read resources: read everything every run, reconcile every run
# ---------------------------------------------------------------------------


def _full_read(
    table: str, key: Sequence[str], rows: Iterator[Row], max_delete_share: float
) -> Iterator[Row]:
    now = pendulum.now("UTC")
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        k = tuple(row[c] for c in key)
        if k in seen:
            continue
        seen.add(k)
        row.setdefault("_source_created_at", None)
        row.setdefault("_source_updated_at", None)
        row.update(_is_deleted=False, _deleted_at=None, _extracted_at=now)
        yield row
    yield from tombstones(table, key, seen, now, max_delete_share=max_delete_share)


def _gift_batches(client: RESTClient, tz: str, max_delete_share: float) -> Any:
    @dlt.resource(
        name="gift_batches",
        primary_key="id",
        write_disposition=_UPSERT,
        columns={**standard_columns(), **columns(text=("id", "batch_number", "added_by"))},
    )
    def _r() -> Iterator[Row]:
        def rows() -> Iterator[Row]:
            for raw in iter_rows(
                client,
                GIFT_BATCHES_PATH,
                resource="gift_batches",
                params={"limit": 500},
                paginator=offset_paginator(limit=500),
                data_selector="giftbatches",
            ):
                yield {**raw, "_source_created_at": to_utc(raw.get("date_added"), tz)}

        yield from _full_read("gift_batches", ("id",), rows(), max_delete_share)

    return _r


def _lists(client: RESTClient, tz: str, max_delete_share: float) -> Any:
    @dlt.resource(
        name="lists",
        primary_key="id",
        write_disposition=_UPSERT,
        columns={**standard_columns(), **columns(text=("id", "list_type", "name"))},
    )
    def _r() -> Iterator[Row]:
        def rows() -> Iterator[Row]:
            for list_type in LIST_TYPES:
                for raw in iter_rows(
                    client,
                    LISTS_PATH,
                    resource="lists",
                    params={"list_type": list_type},
                    paginator=SinglePagePaginator(),
                ):
                    yield {
                        **raw,
                        "list_type": list_type,
                        "_source_updated_at": to_utc(raw.get("date_modified"), tz),
                    }

        yield from _full_read("lists", ("id",), rows(), max_delete_share)

    return _r


def _field_definitions(client: RESTClient, max_delete_share: float) -> Any:
    """Custom field categories per record type (§7.3). RE custom fields are rows, keyed by
    category name, so ``field_key`` is the category name used in ``*_custom_fields.category``.
    """

    @dlt.resource(
        name="field_definitions",
        primary_key=("object_type", "field_key"),
        write_disposition=_UPSERT,
        columns={
            **standard_columns(),
            **columns(
                text=("object_type", "field_key", "label", "data_type", "references_object_type"),
                boolean=("is_custom", "is_archived"),
                json_=("raw",),
            ),
        },
    )
    def _r() -> Iterator[Row]:
        def rows() -> Iterator[Row]:
            for record_type in CUSTOM_FIELD_RECORD_TYPES:
                for cat in iter_rows(
                    client,
                    CUSTOM_FIELD_CATEGORIES_PATH,
                    resource="field_definitions",
                    params={
                        "record_type": record_type,
                        "include_inactive": "true",
                        "limit": CUSTOM_FIELD_CATEGORY_LIMIT,
                    },
                    paginator=offset_paginator(limit=CUSTOM_FIELD_CATEGORY_LIMIT),
                    data_selector="custom_field_categories",
                ):
                    yield {
                        "object_type": record_type.lower(),
                        "field_key": cat.get("description"),
                        "label": cat.get("description"),
                        "data_type": cat.get("type_of_data"),
                        "is_custom": True,
                        "is_archived": bool(cat.get("inactive")),
                        "references_object_type": (
                            f"code_table:{cat['code_table_name']}"
                            if cat.get("code_table_name")
                            else None
                        ),
                        "raw": cat,
                    }

        yield from _full_read(
            "field_definitions", ("object_type", "field_key"), rows(), max_delete_share
        )

    return _r


def _field_options(client: RESTClient, max_delete_share: float) -> Any:
    """Code table entries (§7.3). RE stores the entry's description on records, so ``value``
    and ``label`` are both the description.
    """

    @dlt.resource(
        name="field_options",
        primary_key=("object_type", "field_key", "value"),
        write_disposition=_UPSERT,
        columns={
            **standard_columns(),
            **columns(
                text=("object_type", "field_key", "value", "label"),
                boolean=("is_archived",),
                json_=("raw",),
            ),
            "display_order": {"data_type": "bigint", "nullable": True},
        },
    )
    def _r() -> Iterator[Row]:
        def rows() -> Iterator[Row]:
            for table in iter_rows(
                client,
                CODE_TABLES_PATH,
                resource="code_tables",
                params={"limit": 500},
                paginator=offset_paginator(limit=500, total_path=None),
                data_selector=None,  # type: ignore[arg-type]
            ):
                table_id = _first(table, "code_table_id", "code_tables_id", "id")
                table_name = _first(table, "name", "table_name")
                for entry in iter_rows(
                    client,
                    CODE_TABLE_ENTRIES_PATH.format(code_table_id=table_id),
                    resource="field_options",
                    params={"include_inactive": "true", "limit": 500},
                    paginator=offset_paginator(limit=500, total_path=None),
                    data_selector=None,  # type: ignore[arg-type]
                ):
                    value = _first(entry, "long_description", "description", "short_description")
                    if value is None:
                        continue
                    yield {
                        "object_type": "code_table",
                        "field_key": table_name,
                        "value": value,
                        "label": value,
                        "display_order": entry.get("sequence"),
                        "is_archived": bool(entry.get("inactive")),
                        "raw": {**entry, "code_table_id": table_id},
                    }

        yield from _full_read(
            "field_options", ("object_type", "field_key", "value"), rows(), max_delete_share
        )

    return _r


def _currency_configuration(
    configuration: Callable[[], dict[str, Any]], max_delete_share: float
) -> Any:
    @dlt.resource(
        name="currency_configuration",
        primary_key="currency_code",
        write_disposition=_UPSERT,
        columns={**standard_columns(), **columns(text=("currency_code",))},
    )
    def _r() -> Iterator[Row]:
        body = configuration()
        rows = iter([dict(body)] if body.get("currency_code") else [])
        yield from _full_read("currency_configuration", ("currency_code",), rows, max_delete_share)

    return _r


__all__ = ["raisers_edge_source"]
