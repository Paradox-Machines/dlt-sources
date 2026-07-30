"""Attio source helpers — paginator, value transforms, schema hints."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import HTTPError, Request, Response

from paradox_dlt_sources.attio.settings import ATTIO_MAX_PAGE_SIZE

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


class AttioRecordOffsetPaginator(BasePaginator):
    """POST `/v2/objects/{slug}/records/query` limit/offset body pagination.

    Attio's records-query endpoint takes `limit` and `offset` in the POST
    body and returns no pagination cursor. Walk it by seeding `offset: 0`
    and incrementing by `limit` per page; a page shorter than `limit` is
    the last one.

    PAR-1014: an earlier cursor-based paginator watched for
    `pagination.next_cursor`, which this endpoint never sends — so it
    stopped after page one and silently truncated `companies` and
    `people` to Attio's default page size of 500.
    """

    def __init__(self, page_size: int = ATTIO_MAX_PAGE_SIZE) -> None:
        super().__init__()
        if not 0 < page_size <= ATTIO_MAX_PAGE_SIZE:
            raise ValueError(
                f"page_size must be in 1..{ATTIO_MAX_PAGE_SIZE} "
                f"(Attio's per-request maximum), got {page_size}"
            )
        self._page_size = page_size
        self._offset = 0

    def initial_body(self) -> dict[str, int]:
        """Body for the FIRST request — dlt never calls `update_request` for it.

        Without this the opening page falls back to Attio's server-side
        default limit, which both truncates and desyncs the offset walk.
        """
        return {"limit": self._page_size, "offset": self._offset}

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        if data is None:
            body = response.json() if response.content else {}
            data = body.get("data") or []
        received = len(data)
        # A short page means we've reached the end. A full page might be the
        # exact final page — one extra empty request is the cost of not
        # trusting a total the endpoint doesn't return.
        self._has_next_page = received >= self._page_size
        if self._has_next_page:
            self._offset += received

    def update_request(self, request: Request) -> None:
        body = request.json or {}
        body["limit"] = self._page_size
        body["offset"] = self._offset
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
