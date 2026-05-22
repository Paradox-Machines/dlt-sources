"""Pipedrive source helpers — paginator, row transforms, schema hints."""

from __future__ import annotations

from typing import Any

from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import Request, Response

Row = dict[str, Any]


class PipedrivePaginator(BasePaginator):
    """Pipedrive v1 list endpoints carry pagination metadata at
    `additional_data.pagination`: `{start, limit, more_items_in_collection,
    next_start}`. None of dlt's stock paginators match exactly — close
    cousins assume `next_start` lives at the response root or a known
    `next` URL.
    """

    def __init__(self) -> None:
        super().__init__()
        self._next_start: int | None = None

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body = response.json()
        pagination = (body.get("additional_data") or {}).get("pagination") or {}
        if pagination.get("more_items_in_collection") and "next_start" in pagination:
            self._next_start = pagination["next_start"]
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if self._next_start is None:
            return
        params = dict(request.params or {})
        params["start"] = self._next_start
        request.params = params


# ---------------------------------------------------------------------------
# Schema-hint helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Row transforms
# ---------------------------------------------------------------------------


def _first_value(items: Any) -> Any:
    """Return the `value` field of the first element of a list, or None."""
    if not isinstance(items, list) or not items:
        return None
    first = items[0]
    return first.get("value") if isinstance(first, dict) else None


def flatten_person_contact_arrays(row: Row) -> Row:
    """Lift the first email/phone entry to flat `email_primary`/`phone_primary`
    columns so dlt doesn't create `persons__email`/`persons__phone` child
    tables. Matches the staging model's first-element semantic."""
    return {
        **row,
        "email_primary": _first_value(row.get("email")),
        "phone_primary": _first_value(row.get("phone")),
    }


def _extract_user_ref_id(value: Any) -> Any:
    """Pipedrive returns user-reference fields as either a scalar id or a
    nested `{id, name, email, ...}` object depending on the endpoint. Pull
    `.id` out of the object form so the column lands as a flat bigint —
    same shape as Airbyte's legacy `_airbyte_data.<field>` BIGINT."""
    if isinstance(value, dict):
        return value.get("id")
    return value


def flatten_deal_user_refs(row: Row) -> Row:
    """`/v1/deals` returns `user_id` and `creator_user_id` as nested user
    objects. Flatten to scalar ids so the bigint column hint applies."""
    return {
        **row,
        "user_id": _extract_user_ref_id(row.get("user_id")),
        "creator_user_id": _extract_user_ref_id(row.get("creator_user_id")),
    }


def flatten_person_owner_ref(row: Row) -> Row:
    """`/v1/persons` returns `owner_id` as a nested user object (unlike
    `org_id` on the same endpoint which is a scalar). Flatten to id."""
    return {
        **row,
        "owner_id": _extract_user_ref_id(row.get("owner_id")),
    }
