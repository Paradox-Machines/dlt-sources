"""Attio source helpers — paginator, value transforms, schema hints."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import HTTPError, Request, Response

LOGGER = logging.getLogger(__name__)

Row = dict[str, Any]
_HTTP_FORBIDDEN = 403


def nullable_column(data_type: str) -> dict[str, Any]:
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a `@dlt.resource(columns=...)` map of nullable text + bigint hints.

    dlt drops all-NULL columns from the load schema, breaking downstream
    consumers that reference them. Up-front hints guarantee the column
    exists regardless of data shape.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    return out


class AttioRecordCursorPaginator(BasePaginator):
    """POST `/v2/objects/{slug}/records/query` body-cursor pagination.

    Attio returns `pagination.next_cursor` (string) when there are more
    records; pass it back as `cursor` in the next POST body. Loop ends
    when the field is absent.
    """

    def __init__(self) -> None:
        super().__init__()
        self._cursor: str | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body = response.json() if response.content else {}
        next_cursor = (body.get("pagination") or {}).get("next_cursor")
        if next_cursor:
            self._cursor = next_cursor
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._cursor is None:
            return
        body = request.json or {}
        body["cursor"] = self._cursor
        request.json = body


def active_scalar(entry: dict[str, Any]) -> Any:
    """Extract the canonical scalar from an active `values.<attr>` entry.

    Attio attribute types each pick a different field name for the scalar
    (value/email_address/domain/full_name/original_phone_number/
    formatted_address/currency_value/referenced_actor_id/target_record_id,
    plus status.title). Returns the first one found, or None.
    """
    for key in (
        "value",
        "email_address",
        "domain",
        "full_name",
        "original_phone_number",
        "formatted_address",
        "currency_value",
        "referenced_actor_id",
        "target_record_id",
    ):
        if key in entry:
            return entry[key]
    status = entry.get("status")
    if isinstance(status, dict):
        return status.get("title")
    return None


def promote_active_values(row: Row) -> Row:
    """Hoist `values.<attr>[active].<scalar>` to top-level columns.

    Each Attio record has `values: {attr_slug: [{active_from, active_until,
    ...}, ...]}` where the "current" entry has `active_until is None`.
    Promotes that entry's scalar to `row[attr_slug]`. Idempotent.
    """
    values = row.get("values")
    if not isinstance(values, dict):
        return row
    for attr_name, entries in values.items():
        if not isinstance(entries, list) or not entries:
            continue
        active = next(
            (e for e in entries if isinstance(e, dict) and e.get("active_until") is None),
            None,
        )
        if active is None:
            continue
        scalar = active_scalar(active)
        if scalar is not None and attr_name not in row:
            row[attr_name] = scalar
    return row


def skip_on_forbidden(resource_name: str, scopes: str, gen: Iterator[Any]) -> Iterator[Any]:
    """Yield from `gen`, but soft-fail on HTTP 403 by logging + returning.

    Attio API keys carry per-endpoint OAuth scopes. Missing scope → 403.
    Without this wrapper the entire pipeline aborts even if other
    resources could have succeeded. Log + skip instead so operators see
    an actionable "re-mint with X scope" message.
    """
    try:
        yield from gen
    except HTTPError as exc:
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == _HTTP_FORBIDDEN:
            LOGGER.warning(
                "Attio resource %r returned 403 Forbidden — the API key is "
                "missing the required scope(s): %s. Skipping this resource; "
                "other resources continue. Re-mint the key with the missing "
                "scope(s) at Attio → Workspace settings → Developers → API "
                "tokens and re-run.",
                resource_name,
                scopes,
            )
            return
        raise
