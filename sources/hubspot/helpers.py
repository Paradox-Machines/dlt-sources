"""HubSpot source helpers — paginators, schema hints, change capture, deletes."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import dlt
import pendulum
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


def nullable_column(data_type: str) -> dict[str, Any]:
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=...)`` map of nullable text + bigint hints.

    dlt drops all-NULL columns from the load schema, breaking downstream
    consumers that reference them.  Up-front hints guarantee the column
    exists regardless of data shape.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    return out


def delete_columns() -> dict[str, dict[str, Any]]:
    """Soft-delete columns (spec §10.1) carried by every reconciled resource."""
    return {
        "_is_deleted": {"data_type": "bool", "nullable": True},
        "_deleted_at": nullable_column("timestamp"),
    }


def to_datetime(value: Any) -> pendulum.DateTime | None:
    """A cursor value as UTC datetime: ISO strings, or epoch milliseconds (v1 API)."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return pendulum.from_timestamp(value / 1000, tz="UTC")
    parsed: pendulum.DateTime = pendulum.parse(str(value))  # type: ignore[assignment]
    return parsed.in_tz("UTC")


class ChangeWindow:
    """Cursor bookkeeping for one full-walk resource run.

    Every run pages the whole collection, so the cursor only decides which rows
    are worth landing again. Rows modified after ``cursor - lookback`` are
    emitted. The saved cursor never passes the moment the run started: a record
    edited while the walk is in progress (after its page was read) carries a
    later timestamp and is picked up by the next run. The lookback covers clock
    skew between HubSpot and this process.

    State is written only by :meth:`commit`, after the walk finishes, so a
    failed run saves nothing. The first run under this class adopts the
    ``last_value`` of the ``dlt.sources.incremental`` it replaced, so no full
    reload happens on upgrade.
    """

    def __init__(self, state: dict[str, Any], cursor_path: str, lookback: timedelta) -> None:
        self._state = state
        saved = state.get("cursor")
        if saved is None:
            saved = state.get("incremental", {}).get(cursor_path, {}).get("last_value")
        self.cursor = to_datetime(saved)
        self.started_at = pendulum.now("UTC")
        self._floor = None if self.cursor is None else self.cursor - lookback
        self._high = self.cursor
        self.seen: set[Any] = set()

    def admit(self, key: Any, updated: Any) -> bool:
        """Record ``key`` as present; ``True`` when the row should be landed."""
        self.seen.add(key)
        ts = to_datetime(updated)
        if ts is not None and (self._high is None or ts > self._high):
            self._high = ts
        return self._floor is None or ts is None or ts > self._floor

    def commit(self) -> None:
        """Persist the new cursor: the newest timestamp seen, capped at the run start."""
        if self._high is None:
            return
        new = min(self._high, self.started_at)
        if self.cursor is not None:
            new = max(new, self.cursor)
        self._state["cursor"] = new.isoformat()


# ---------------------------------------------------------------------------
# Deletes (spec §12.2 method 4: key reconciliation against the destination)
# ---------------------------------------------------------------------------

_TOMBSTONE_LOOKUP_CHUNK = 200


def _sql_literal(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _query(pipeline: Any, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    with pipeline.sql_client() as client, client.execute_query(sql) as cursor:
        names = [d[0] for d in cursor.description]
        return names, [tuple(r) for r in cursor.fetchall()]


def tombstones(
    table: str,
    key: str,
    seen: set[Any],
    now: pendulum.DateTime,
    *,
    max_delete_share: float,
) -> Iterator[Row]:
    """Soft-delete rows for every live destination key absent from ``seen``.

    The destination is append-only (one row per landed version), so "live"
    means the newest version of a key (by ``_dlt_load_id``) is not already a
    tombstone. Each tombstone repeats that newest version with ``_is_deleted``
    set, so downstream latest-per-key reads keep the record's last values and
    can drop it. Refuses to tombstone more than ``max_delete_share`` of the
    live keys in one run: an empty or truncated read must not wipe a table.

    Does nothing until the table has landed with ``_is_deleted`` (the first run
    after upgrade only adds the column).
    """
    pipeline = dlt.current.pipeline()
    if pipeline.default_schema_name is None:
        return
    schema_table = pipeline.default_schema.tables.get(table)
    if not schema_table or "_is_deleted" not in schema_table.get("columns", {}):
        return
    with pipeline.sql_client() as client:
        qualified = client.make_qualified_table_name(table)
    latest = (
        f"SELECT *, row_number() OVER (PARTITION BY {key} "
        f"ORDER BY try_cast(_dlt_load_id AS DOUBLE) DESC, _dlt_id DESC) AS _hs_rn "
        f"FROM {qualified}"
    )
    try:
        _, live_rows = _query(
            pipeline,
            f"SELECT {key} FROM ({latest}) WHERE _hs_rn = 1 AND NOT coalesce(_is_deleted, false)",
        )
    except DatabaseUndefinedRelation:
        return
    live = {r[0] for r in live_rows}
    missing = sorted(live - seen)
    if not missing:
        return
    if len(missing) / len(live) > max_delete_share:
        raise RuntimeError(
            f"{table}: {len(missing)} of {len(live)} live keys are missing from the source "
            f"read, above max_delete_share={max_delete_share}. Refusing to tombstone them. "
            "Check the private app's scopes, or raise max_delete_share."
        )
    json_columns = {
        name
        for name, col in pipeline.default_schema.get_table_columns(table).items()
        if col.get("data_type") == "json"
    }
    for start in range(0, len(missing), _TOMBSTONE_LOOKUP_CHUNK):
        chunk = ", ".join(_sql_literal(k) for k in missing[start : start + _TOMBSTONE_LOOKUP_CHUNK])
        names, rows = _query(
            pipeline, f"SELECT * FROM ({latest}) WHERE _hs_rn = 1 AND {key} IN ({chunk})"
        )
        for values in rows:
            row = {
                n: (json.loads(v) if n in json_columns and isinstance(v, str) else v)
                for n, v in zip(names, values, strict=True)
                if not n.startswith("_dlt_") and n != "_hs_rn"
            }
            row["_is_deleted"] = True
            row["_deleted_at"] = now
            yield row


class EngagementsOffsetPaginator(BasePaginator):
    """Engagements v1 ``{results, hasMore, offset}`` pagination.

    The legacy ``/engagements/v1/engagements/paged`` endpoint emits the
    next-page token as the ``offset`` field in the response body, and
    signals more pages via ``hasMore: true``.  dlt's stock
    ``OffsetPaginator`` increments offset client-side by ``limit``, which
    only works for dense pages.  This paginator reads the server-supplied
    offset directly, which is the correct approach for the v1 endpoint.
    """

    def __init__(self) -> None:
        super().__init__()
        self._offset: int | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body: dict[str, Any] = response.json()
        if body.get("hasMore") and "offset" in body:
            self._offset = int(body["offset"])
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._offset is None:
            return
        if request.params is None:
            request.params = {}
        request.params["offset"] = self._offset
