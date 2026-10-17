"""Raiser's Edge NXT source helpers: auth, paging, row transforms, cursor and delete reconciliation."""

from __future__ import annotations

import base64
import json
import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import date, datetime, timedelta
from typing import Any

import dlt
import pendulum
import requests as _requests
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    JSONLinkPaginator,
    OffsetPaginator,
)
from requests import PreparedRequest, Response
from requests.auth import AuthBase

from .settings import FULL_READ_FLOOR, PAGE_LIMIT

logger = logging.getLogger(__name__)

Row = dict[str, Any]

_REFRESH_SAFETY_SECONDS = 60
_NAIVE_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$")
_TOMBSTONE_LOOKUP_CHUNK = 200
_HTTP_FORBIDDEN = 403


class SkyQuotaExceededError(RuntimeError):
    """The SKY API daily call quota is spent. Quotas reset at 00:00 UTC."""


# ---------------------------------------------------------------------------
# Column hints
# ---------------------------------------------------------------------------


def columns(
    *,
    text: Iterable[str] = (),
    decimal: Iterable[str] = (),
    boolean: Iterable[str] = (),
    timestamp: Iterable[str] = (),
    date_: Iterable[str] = (),
    json_: Iterable[str] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=...)`` map of nullable hints, one dict per column."""
    out: dict[str, dict[str, Any]] = {}
    for names, data_type in (
        (text, "text"),
        (decimal, "decimal"),
        (boolean, "bool"),
        (timestamp, "timestamp"),
        (date_, "date"),
        (json_, "json"),
    ):
        for name in names:
            out[name] = {"data_type": data_type, "nullable": True}
    return out


def standard_columns() -> dict[str, dict[str, Any]]:
    """Spec §10.1 core columns."""
    out = columns(
        timestamp=("_source_created_at", "_source_updated_at", "_deleted_at", "_extracted_at")
    )
    out["_is_deleted"] = {"data_type": "bool", "nullable": False}
    return out


# ---------------------------------------------------------------------------
# OAuth 2.0
# ---------------------------------------------------------------------------


class SkyOAuth2Auth(AuthBase):
    """SKY API OAuth 2.0 refresh-token grant plus the subscription key header.

    SKY API offers only the authorization code flow. One interactive consent yields a
    refresh token valid for 365 days. By default every refresh rotates it;
    ``preserve_refresh_token=True`` keeps the same value and expiry, so the stored secret
    never changes. When rotation is on, ``on_token_rotation`` receives each new token and
    the caller MUST persist it.

    Args:
        token_url: SKY OAuth token endpoint.
        client_id: SKY application ID.
        client_secret: SKY application secret.
        refresh_token: Current refresh token.
        subscription_key: ``Bb-Api-Subscription-Key`` from the developer subscription.
        preserve_refresh_token: Keep the refresh token value on refresh.
        on_token_rotation: Called with the new refresh token when it rotates.
    """

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        subscription_key: str,
        preserve_refresh_token: bool,
        on_token_rotation: Callable[[str], None],
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._subscription_key = subscription_key
        self._preserve = preserve_refresh_token
        self._on_token_rotation = on_token_rotation
        self._access_token: str | None = None
        self._expires_at = 0.0
        self.environment_id: str | None = None

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self._access_token is None or time.time() >= self._expires_at - _REFRESH_SAFETY_SECONDS:
            self._refresh()
        request.headers["Authorization"] = f"Bearer {self._access_token}"
        request.headers["Bb-Api-Subscription-Key"] = self._subscription_key
        return request

    def _refresh(self) -> None:
        basic = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        data = {"grant_type": "refresh_token", "refresh_token": self._refresh_token}
        if self._preserve:
            data["preserve_refresh_token"] = "true"
        response = _requests.post(
            self._token_url,
            headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
            data=data,
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = body["access_token"]
        self._expires_at = time.time() + float(body["expires_in"])
        self.environment_id = body.get("environment_id")
        new_refresh = body.get("refresh_token")
        if new_refresh and new_refresh != self._refresh_token:
            self._refresh_token = new_refresh
            self._on_token_rotation(new_refresh)


def raise_on_quota(response: Response, *args: Any, **kwargs: Any) -> None:
    """Turn the SKY daily-quota 403 into a readable error instead of a bare HTTPError."""
    if response.status_code == _HTTP_FORBIDDEN and "quota" in response.text.lower():
        raise SkyQuotaExceededError(
            f"SKY API daily quota exhausted (resets 00:00 UTC, Retry-After="
            f"{response.headers.get('Retry-After')}s): {response.text[:200]}"
        )


def make_client(base_url: str, auth: SkyOAuth2Auth) -> RESTClient:
    """REST client with dlt's retrying session (429/5xx, honours ``Retry-After``)."""
    return RESTClient(base_url=base_url, auth=auth, headers={"Accept": "application/json"})


# ---------------------------------------------------------------------------
# Paging
# ---------------------------------------------------------------------------


class SkyLinkPaginator(JSONLinkPaginator):
    """Follow ``next_link``; stop on a missing link or an empty ``value`` page."""

    def __init__(self) -> None:
        super().__init__(next_url_path="next_link")

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        super().update_state(response, data)
        if not data:
            self._next_reference = None


def offset_paginator(limit: int = PAGE_LIMIT, total_path: str | None = "count") -> BasePaginator:
    """Limit/offset paging; stops at ``count`` or on an empty page."""
    return OffsetPaginator(limit=limit, total_path=total_path, stop_after_empty_page=True)


class _CountCheck:
    """Spec §17: compare the source's reported total with rows extracted (warn for APIs)."""

    def __init__(self, resource: str) -> None:
        self.resource = resource
        self.reported: int | None = None
        self.extracted = 0

    def hook(self, response: Response, *args: Any, **kwargs: Any) -> None:
        if self.reported is None and response.ok:
            try:
                count = response.json().get("count")
            except ValueError:
                return
            if isinstance(count, int):
                self.reported = count

    def check(self) -> None:
        if self.reported is not None and self.reported != self.extracted:
            logger.warning(
                "%s: source reported %s rows, extracted %s",
                self.resource,
                self.reported,
                self.extracted,
            )


def iter_rows(
    client: RESTClient,
    path: str,
    *,
    resource: str,
    params: dict[str, Any],
    paginator: BasePaginator,
    data_selector: str = "value",
) -> Iterator[Row]:
    """Yield every row of a list endpoint, then run the §17 count check."""
    counter = _CountCheck(resource)
    for page in client.paginate(
        path,
        params=params,
        paginator=paginator,
        data_selector=data_selector,
        hooks={"response": [raise_on_quota, counter.hook]},
    ):
        for row in page:
            counter.extracted += 1
            yield row
    counter.check()


def change_params(base: dict[str, Any], floor: pendulum.DateTime | None) -> dict[str, Any]:
    """Query parameters for a `last_modified` read starting at ``floor`` (``None`` = full)."""
    params = {**base, "limit": PAGE_LIMIT}
    params["last_modified"] = floor.isoformat() if floor is not None else FULL_READ_FLOOR
    return params


def paginator_for(paging: str) -> BasePaginator:
    """Paginator for an endpoint's paging style."""
    return SkyLinkPaginator() if paging == "link" else offset_paginator()


# ---------------------------------------------------------------------------
# Row transforms
# ---------------------------------------------------------------------------


def to_utc(value: Any, tz: str) -> pendulum.DateTime | None:
    """Parse a SKY date-time. Offsets are honoured; naive values are read in ``tz``."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = pendulum.instance(value)
    else:
        parsed = pendulum.parse(str(value), tz=tz)  # type: ignore[assignment]
    if not isinstance(parsed, pendulum.DateTime):
        raise ValueError(f"not a date-time: {value!r}")
    return parsed.in_timezone("UTC")


def to_date(value: Any) -> date | None:
    """Date part of a date-only RE field sent as a date-time (``2024-05-01T00:00:00``)."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def flatten_objects(row: Row) -> Row:
    """Flatten top-level objects one level into ``<field>__<key>`` columns.

    The source uses ``max_table_nesting=0`` so lists land as ``json`` (never dlt child
    tables). Flattening objects one level keeps money (``amount__value``) and the inline
    primary email/phone/address as typed columns; deeper values stay ``json``.
    """
    out: Row = {}
    for key, value in row.items():
        if isinstance(value, dict):
            for sub, sub_value in value.items():
                out[f"{key}__{sub}"] = sub_value
        else:
            out[key] = value
    return out


def normalize_row(
    raw: Row,
    *,
    tz: str,
    date_fields: Sequence[str],
    extracted_at: pendulum.DateTime,
    currency_code: str | None = None,
) -> Row:
    """Apply §8.2 fidelity rules and add the §10.1 standard columns.

    Objects are flattened one level (see ``flatten_objects``). Date-only fields become
    ``date``. Other top-level naive date-times are read in the
    declared source timezone and stored as UTC. Values are otherwise untouched.
    """
    row: Row = {}
    for key, value in flatten_objects(raw).items():
        if key in date_fields:
            row[key] = to_date(value)
        elif isinstance(value, str) and _NAIVE_DATETIME.match(value):
            row[key] = to_utc(value, tz)
        else:
            row[key] = value
    row["_source_created_at"] = to_utc(raw.get("date_added"), tz)
    row["_source_updated_at"] = to_utc(raw.get("date_modified"), tz)
    row["_is_deleted"] = False
    row["_deleted_at"] = None
    row["_extracted_at"] = extracted_at
    if currency_code is not None:
        row["currency_code"] = currency_code
    return row


# ---------------------------------------------------------------------------
# Cursor state (§11.2)
# ---------------------------------------------------------------------------


class ChangeWindow:
    """Cursor bookkeeping for one resource run.

    Holds the high-water ``_source_updated_at`` and drops repeated keys so an upsert batch
    never holds the same key twice (§5.2). A key seen twice changed mid-read; the later
    copy is dropped and the saved cursor is held at its timestamp so the next run re-reads
    it. State is written only when the generator finishes, so a failed run saves nothing.
    """

    def __init__(self, state: dict[str, Any], lookback: timedelta) -> None:
        self._state = state
        saved = state.get("cursor")
        self.cursor: pendulum.DateTime | None = (
            pendulum.parse(saved) if saved else None  # type: ignore[assignment]
        )
        self._lookback = lookback
        self._high = self.cursor
        self._held_back: pendulum.DateTime | None = None
        self.seen: set[tuple[Any, ...]] = set()

    def floor(self, start_date: pendulum.DateTime | None) -> pendulum.DateTime | None:
        """Inclusive `last_modified` floor: saved cursor minus lookback, else ``start_date``."""
        if self.cursor is not None:
            return self.cursor - self._lookback
        return start_date

    def admit(self, key: tuple[Any, ...], updated_at: pendulum.DateTime | None) -> bool:
        """Record a row; ``False`` when its key was already emitted in this run."""
        if key in self.seen:
            if updated_at is not None and (self._held_back is None or updated_at < self._held_back):
                self._held_back = updated_at
            return False
        self.seen.add(key)
        if updated_at is not None and (self._high is None or updated_at > self._high):
            self._high = updated_at
        return True

    def commit(self) -> None:
        """Persist the new cursor to resource state."""
        new = self._high
        if self._held_back is not None and (new is None or self._held_back < new):
            new = self._held_back
        if new is not None:
            self._state["cursor"] = new.isoformat()


def reconciliation_due(
    state: dict[str, Any], interval: timedelta | None, now: pendulum.DateTime
) -> bool:
    """Whether §12.2 method 4 runs now. Never on the very first run (it is a full read)."""
    if interval is None:
        return False
    last = state.get("last_reconciled_at")
    if last is None:
        return False
    return bool(now - pendulum.parse(last) >= interval)  # type: ignore[operator]


# ---------------------------------------------------------------------------
# Deletes (§12.2 method 4: key reconciliation against the destination)
# ---------------------------------------------------------------------------


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _destination_table(table: str) -> Any | None:
    """The pipeline's dataset when ``table`` exists in it, else ``None``."""
    pipeline = dlt.current.pipeline()
    if pipeline.default_schema_name is None:
        return None
    schema_table = pipeline.default_schema.tables.get(table)
    if not schema_table or "_is_deleted" not in schema_table.get("columns", {}):
        return None
    return pipeline


def tombstones(
    table: str,
    key_columns: Sequence[str],
    seen: set[tuple[Any, ...]],
    now: pendulum.DateTime,
    *,
    max_delete_share: float,
) -> Iterator[Row]:
    """Yield soft-delete rows for every live destination key absent from ``seen``.

    Each tombstone is the last loaded row with ``_is_deleted`` set, so an upsert keeps the
    record's values (§12.1). Refuses to delete more than ``max_delete_share`` of the live
    keys in one run: an empty or truncated read must not wipe a table.

    Args:
        table: Destination table, equal to the resource name.
        key_columns: Primary key columns.
        seen: Keys present in the source read that just completed.
        now: Detection time, used as ``_deleted_at``.
        max_delete_share: Largest share of live keys one run may tombstone.
    """
    pipeline = _destination_table(table)
    if pipeline is None:
        return
    dataset = pipeline.dataset()
    keys_sql = ", ".join(key_columns)
    live_sql = f"SELECT {keys_sql} FROM {table} WHERE _is_deleted = false"
    live = {tuple(r) for r in dataset.query(live_sql).fetchall()}
    missing = sorted(live - seen)
    if not missing:
        return
    if live and len(missing) / len(live) > max_delete_share:
        raise RuntimeError(
            f"{table}: {len(missing)} of {len(live)} live keys are missing from the source "
            f"read, above max_delete_share={max_delete_share}. Refusing to tombstone them. "
            "Check the consenting user's permissions, or raise max_delete_share."
        )
    json_columns = {
        name
        for name, col in pipeline.default_schema.get_table_columns(table).items()
        if col.get("data_type") == "json"
    }
    for start in range(0, len(missing), _TOMBSTONE_LOOKUP_CHUNK):
        chunk = missing[start : start + _TOMBSTONE_LOOKUP_CHUNK]
        match = " OR ".join(
            "("
            + " AND ".join(
                f"{c} = {_sql_literal(v)}" for c, v in zip(key_columns, key, strict=True)
            )
            + ")"
            for key in chunk
        )
        relation = dataset.query(f"SELECT * FROM {table} WHERE {match}")
        names = relation.columns
        for values in relation.fetchall():
            row = {
                n: (json.loads(v) if n in json_columns and isinstance(v, str) else v)
                for n, v in zip(names, values, strict=True)
                if not n.startswith("_dlt_")
            }
            row["_is_deleted"] = True
            row["_deleted_at"] = now
            row["_extracted_at"] = now
            yield row


def finish_reconciliation(state: dict[str, Any], now: pendulum.DateTime) -> None:
    """Record a completed reconciliation."""
    state["last_reconciled_at"] = now.isoformat()
