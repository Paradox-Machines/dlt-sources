"""Unit4 ERP dlt source — finance and personnel objects from the U4 ERPx Object API.

Resources
---------
One dlt resource per (object, company), named ``{object}_{company}`` and
writing to the shared ``{object}`` table.  Per-company resources give each
company its own cursor and reconciliation clock.  Every row carries
``_source_account_id`` (the ``companyId`` it was read with), which leads the
primary key.  The object table lives in ``settings.OBJECTS``; payroll objects
are ``restricted`` and load only when named in ``resources``.

``field_definitions`` holds one row per (company, object, field) from the
run-time JSON Schema each object serves (``Accept: application/schema+json``),
custom fields included.

Paging and change capture
-------------------------
* Keyset paging on the object's key fields (never ``offset``).
* Objects with ``lastUpdated`` are incremental: the server-side filter is
  ``lastUpdated/updatedAt ge (cursor - lookback)``.  The cursor is the
  highest ``updatedAt`` loaded by the *previous* run (the start value), kept
  in resource state and written only after the walk completes; dlt commits
  state with a successful load only.
* Objects without ``lastUpdated`` are read in full every run.

Deletes
-------
Key reconciliation (CONNECTOR_SPEC §12.2 method 4).  A full read's keys are
the live set; incremental objects also re-read their keys only (``select`` of
the key fields) every ``delete_reconciliation_interval``.  Stored rows whose
key is not live are re-emitted with ``_is_deleted = true``; a row that comes
back is upserted with ``_is_deleted = false``.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    KeyTuple,
    Row,
    check_reconciliation,
    columns,
    iter_pages,
    key_select,
    key_tuple,
    make_auth,
    make_client,
    normalized_names,
    parse_timestamp,
    primary_key,
    read_stored_rows,
    timestamp_literal,
    tombstones,
    transform_row,
)
from .settings import (
    DEFAULT_LOOKBACK_HOURS,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_REQUESTS_PER_MINUTE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_RECONCILIATION_INTERVAL_HOURS,
    DEFAULT_SELECT,
    FIELD_DEFINITIONS,
    OBJECTS,
    OBJECTS_BY_NAME,
    SCHEMA_MEDIA_TYPE,
    SOURCE_ACCOUNT_COLUMN,
    STANDARD_COLUMNS,
    UPDATED_AT_PATH,
    UnitObject,
)

__all__ = ["unit4_erp_source"]

logger = logging.getLogger(__name__)

_CURSOR_STATE_KEY = "updated_at"
# Nested schema levels flattened into field_definitions rows.
_MAX_SCHEMA_DEPTH = 3
_RECONCILED_STATE_KEY = "reconciled_at"

_FIELD_DEFINITION_KEYS = [SOURCE_ACCOUNT_COLUMN, "object_type", "field_key"]
_FIELD_DEFINITION_COLUMNS: Mapping[str, str] = {
    "object_type": "text",
    "field_key": "text",
    "label": "text",
    "data_type": "text",
    "is_custom": "bool",
    "is_archived": "bool",
    "is_filterable": "bool",
    "is_mandatory": "bool",
    "references_object_type": "text",
    "raw": "json",
}


@dlt.source(name="unit4_erp", max_table_nesting=0)
def unit4_erp_source(
    credentials: dict[str, Any] = dlt.secrets.value,
    base_url: str = dlt.config.value,
    company_ids: list[str] = dlt.config.value,
    start_date: str | None = None,
    lookback: dt.timedelta = dt.timedelta(hours=DEFAULT_LOOKBACK_HOURS),  # noqa: B008
    resources: list[str] | None = None,
    delete_reconciliation_interval: dt.timedelta | None = dt.timedelta(  # noqa: B008
        hours=DEFAULT_RECONCILIATION_INTERVAL_HOURS
    ),
    source_timezone: str = "UTC",
    page_size: int = DEFAULT_PAGE_SIZE,
    select: str = DEFAULT_SELECT,
    max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> list[DltResource]:
    """Load Unit4 ERP objects for one or more companies of one tenant.

    Args:
        credentials: ``type`` = ``oauth_client_credentials`` (``token_url``,
            ``client_id``, ``client_secret``, optional ``scope``) or ``basic``
            (``username``, ``password``).
        base_url: Tenant and environment API root, without the ``/v1`` part.
        company_ids: ERP ``companyId`` values to load.
        start_date: ISO date or timestamp floor for the first run of incremental
            objects.  ``None`` loads full history.
        lookback: Overlap subtracted from the cursor on every incremental run.
        resources: Resource names to load.  ``None`` loads every unrestricted
            object plus ``field_definitions``; restricted (payroll) objects load
            only when named.
        delete_reconciliation_interval: How often incremental objects re-read
            their keys to detect deletes.  ``None`` disables that (full-read
            objects still reconcile every run).
        source_timezone: IANA zone of timestamps that arrive without an offset.
        page_size: Rows per call (at most 50,000).
        select: ``select`` sent on data reads (``*.*`` includes custom fields).
        max_requests_per_minute: Client-side ceiling below the tenant's 500/min;
            ``0`` disables throttling.
        max_attempts: Attempts per request on 429 / 5xx / connection errors.

    Returns:
        One resource per (object, company), plus ``field_definitions`` per company.

    Raises:
        ValueError: On invalid credentials, companies, dates or resource names.
    """
    if not company_ids:
        raise ValueError("unit4_erp: `company_ids` must list at least one company")
    if len(set(company_ids)) != len(company_ids):
        raise ValueError("unit4_erp: `company_ids` has duplicates")
    tz = ZoneInfo(source_timezone)
    start = parse_timestamp(start_date, tz) if start_date else None
    objects, with_definitions = _select_resources(resources)
    client = make_client(
        make_auth(credentials),
        base_url,
        max_requests_per_minute=max_requests_per_minute,
        max_attempts=max_attempts,
    )
    settings = _RunSettings(
        tz=tz,
        start=start,
        lookback=lookback,
        reconciliation_interval=delete_reconciliation_interval,
        page_size=page_size,
        select=select,
    )

    out: list[DltResource] = []
    for company_id in company_ids:
        out.extend(_object_resource(client, obj, company_id, settings) for obj in objects)
        if with_definitions:
            out.append(_field_definitions_resource(client, objects, company_id, settings))
    return out


class _RunSettings:
    """Per-run options shared by every resource."""

    def __init__(
        self,
        *,
        tz: ZoneInfo,
        start: dt.datetime | None,
        lookback: dt.timedelta,
        reconciliation_interval: dt.timedelta | None,
        page_size: int,
        select: str,
    ) -> None:
        self.tz = tz
        self.start = start
        self.lookback = lookback
        self.reconciliation_interval = reconciliation_interval
        self.page_size = page_size
        self.select = select


def _select_resources(resources: Sequence[str] | None) -> tuple[list[UnitObject], bool]:
    """Resolve the ``resources`` filter: (objects, include field_definitions)."""
    if resources is None:
        return [o for o in OBJECTS if not o.restricted], True
    known = {*OBJECTS_BY_NAME, FIELD_DEFINITIONS}
    unknown = sorted(set(resources) - known)
    if unknown:
        raise ValueError(f"unit4_erp: unknown resource(s) {unknown}; known: {sorted(known)}")
    wanted = set(resources)
    return [o for o in OBJECTS if o.name in wanted], FIELD_DEFINITIONS in wanted


def _resource_name(table: str, company_id: str) -> str:
    return f"{table}_{re.sub(r'[^0-9a-zA-Z]+', '_', company_id).lower()}"


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _reconciliation_due(state: Mapping[str, Any], interval: dt.timedelta | None) -> bool:
    if interval is None:
        return False
    last = state.get(_RECONCILED_STATE_KEY)
    return last is None or _now() - dt.datetime.fromisoformat(last) >= interval


def _deleted_rows(
    table: str, company_id: str, key_names: Sequence[str], live: set[KeyTuple], *, label: str
) -> list[Row]:
    """Stored live rows of ``company_id`` whose key is not in ``live``, as tombstones."""
    stored = read_stored_rows(table, company_id)
    check_reconciliation(stored, live, label=label)
    return tombstones(stored, normalized_names(key_names), live, _now())


def _object_resource(
    client: RESTClient, obj: UnitObject, company_id: str, run: _RunSettings
) -> DltResource:
    """Build the resource reading one object for one company."""
    key_names = primary_key(obj)
    label = f"{obj.name} (company {company_id})"

    def rows() -> Iterator[list[Row]]:
        state = dlt.current.resource_state()
        extracted_at = _now()
        saved = state.get(_CURSOR_STATE_KEY)
        cursor = dt.datetime.fromisoformat(saved) if saved else run.start
        incremental = obj.incremental and cursor is not None
        base_filter = (
            f"{UPDATED_AT_PATH} ge {timestamp_literal(cursor - run.lookback, run.tz)}"
            if incremental and cursor is not None
            else None
        )

        seen: set[KeyTuple] = set()
        high = cursor
        for page in iter_pages(
            client,
            obj,
            company_id,
            page_size=run.page_size,
            select=run.select,
            base_filter=base_filter,
        ):
            out = [transform_row(r, obj, company_id, run.tz, extracted_at) for r in page]
            for row in out:
                seen.add(key_tuple(row, key_names))
                updated = row["_source_updated_at"]
                if updated is not None and (high is None or updated > high):
                    high = updated
            yield out
        logger.info("unit4_erp: %s: %d rows extracted", label, len(seen))

        if not incremental:
            # This run read everything: its keys are the live set.
            deleted = _deleted_rows(obj.name, company_id, key_names, seen, label=label)
            state[_RECONCILED_STATE_KEY] = extracted_at.isoformat()
        elif _reconciliation_due(state, run.reconciliation_interval):
            live = set(seen)
            for page in iter_pages(
                client, obj, company_id, page_size=run.page_size, select=key_select(obj.keys)
            ):
                live.update(
                    key_tuple(transform_row(r, obj, company_id, run.tz, extracted_at), key_names)
                    for r in page
                )
            deleted = _deleted_rows(obj.name, company_id, key_names, live, label=label)
            state[_RECONCILED_STATE_KEY] = extracted_at.isoformat()
        else:
            deleted = []
        if deleted:
            logger.info("unit4_erp: %s: %d rows marked deleted", label, len(deleted))
            yield deleted
        if obj.incremental and high is not None:
            state[_CURSOR_STATE_KEY] = high.isoformat()

    return dlt.resource(
        rows,
        name=_resource_name(obj.name, company_id),
        table_name=obj.name,
        primary_key=key_names,
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        columns=columns(obj),
    )


def _flatten_schema(
    properties: Mapping[str, Any], prefix: str = "", depth: int = 0
) -> Iterator[tuple[str, Mapping[str, Any]]]:
    """Yield (``a/b`` path, definition) for every property, nested ones included."""
    for name, spec in properties.items():
        path = f"{prefix}{name}"
        yield path, spec
        nested = spec.get("properties") or (spec.get("items") or {}).get("properties")
        if nested and depth < _MAX_SCHEMA_DEPTH:
            yield from _flatten_schema(nested, f"{path}/", depth + 1)


def _schema_keys(schema: Mapping[str, Any]) -> set[str]:
    raw = schema.get("x-u4-keys") or (schema.get("x-u4-key") or {}).get("keys") or []
    names = raw.split(",") if isinstance(raw, str) else list(raw)
    return {str(n).strip().rsplit("/", 1)[-1] for n in names if str(n).strip()}


def _definition_rows(
    obj: UnitObject, schema: Mapping[str, Any], company_id: str, extracted_at: dt.datetime
) -> list[Row]:
    declared = {k.path.rsplit("/", 1)[-1] for k in obj.keys}
    served = _schema_keys(schema)
    if served and served != declared:
        logger.warning(
            "unit4_erp: %s (company %s): tenant schema keys %s differ from settings %s",
            obj.name,
            company_id,
            sorted(served),
            sorted(declared),
        )
    rows = []
    for path, spec in _flatten_schema(schema.get("properties") or {}):
        raw = {k: v for k, v in spec.items() if k not in ("properties", "items")}
        dtype = spec.get("type")
        if isinstance(dtype, list):
            dtype = ",".join(str(t) for t in dtype)
        if spec.get("format"):
            dtype = f"{dtype}:{spec['format']}"
        rows.append(
            {
                SOURCE_ACCOUNT_COLUMN: company_id,
                "object_type": obj.name,
                "field_key": path,
                "label": spec.get("title")
                or spec.get("x-u4-description")
                or spec.get("description"),
                "data_type": dtype,
                "is_custom": path.startswith("customFieldGroups"),
                "is_archived": False,
                "is_filterable": bool((spec.get("x-u4-filterable") or {}).get("filterable")),
                "is_mandatory": bool(spec.get("x-u4-mandatory")),
                "references_object_type": None,
                "raw": raw,
                "_source_created_at": None,
                "_source_updated_at": None,
                "_is_deleted": False,
                "_deleted_at": None,
                "_extracted_at": extracted_at,
            }
        )
    return rows


def _field_definitions_resource(
    client: RESTClient, objects: Sequence[UnitObject], company_id: str, run: _RunSettings
) -> DltResource:
    """Build the resource reading every selected object's run-time schema."""
    label = f"{FIELD_DEFINITIONS} (company {company_id})"

    def rows() -> Iterator[list[Row]]:
        extracted_at = _now()
        live: set[KeyTuple] = set()
        for obj in objects:
            response = client.get(
                obj.path,
                params={"companyId": company_id, "select": run.select},
                headers={"Accept": SCHEMA_MEDIA_TYPE},
            )
            response.raise_for_status()
            out = _definition_rows(obj, response.json(), company_id, extracted_at)
            live.update(key_tuple(r, _FIELD_DEFINITION_KEYS) for r in out)
            yield out
        deleted = _deleted_rows(
            FIELD_DEFINITIONS, company_id, _FIELD_DEFINITION_KEYS, live, label=label
        )
        if deleted:
            yield deleted

    hints: dict[str, dict[str, Any]] = {
        name: {"data_type": dtype, "nullable": name != "_is_deleted"}
        for name, dtype in {**STANDARD_COLUMNS, **_FIELD_DEFINITION_COLUMNS}.items()
    }
    return dlt.resource(
        rows,
        name=_resource_name(FIELD_DEFINITIONS, company_id),
        table_name=FIELD_DEFINITIONS,
        primary_key=_FIELD_DEFINITION_KEYS,
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        columns=hints,
    )
