"""Attio dlt source — companies, people, deals, lists, notes."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

from .helpers import (
    AttioRecordCursorPaginator,
    columns,
    promote_active_values,
    skip_on_forbidden,
)
from .settings import (
    ATTIO_API_BASE_URL,
    SCOPES_LISTS,
    SCOPES_NOTES,
    SCOPES_RECORDS,
    STANDARD_OBJECTS,
)

Row = dict[str, Any]

# Per-object column hints — declare the schema each downstream consumer
# reads so dlt always materializes the column even when zero rows / all-NULL.
_COMPANIES_COLUMNS = columns(
    text=(
        "record_id",
        "id__workspace_id",
        "id__object_id",
        "web_url",
        "name",
        "domains",
        "description",
        "categories",
        "primary_location",
        "estimated_arr_usd",
        "employee_range",
        "foundation_date",
        "created_at",
    )
)
_PEOPLE_COLUMNS = columns(
    text=(
        "record_id",
        "id__workspace_id",
        "id__object_id",
        "web_url",
        "name",
        "email_addresses",
        "phone_numbers",
        "job_title",
        "company",
        "created_at",
    )
)
_DEALS_COLUMNS = columns(
    text=(
        "record_id",
        "id__workspace_id",
        "id__object_id",
        "web_url",
        "name",
        "stage",
        "value",
        "owner",
        "associated_company",
        "estimated_close_date",
        "created_at",
    )
)
_LISTS_COLUMNS = columns(
    text=(
        "list_id",
        "id__workspace_id",
        "api_slug",
        "name",
        "workspace_access",
        "created_at",
    )
)
_NOTES_COLUMNS = columns(
    text=(
        "note_id",
        "id__workspace_id",
        "parent_object",
        "parent_record_id",
        "title",
        "content_plaintext",
        "content_markdown",
        "meeting_id",
        "created_by_actor__id",
        "created_by_actor__type",
        "created_at",
    )
)
_RECORD_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "companies": _COMPANIES_COLUMNS,
    "people": _PEOPLE_COLUMNS,
    "deals": _DEALS_COLUMNS,
}


@dlt.source(name="attio")
def attio_source(
    api_key: str = dlt.secrets.value,
    objects: tuple[str, ...] = STANDARD_OBJECTS,
    base_url: str = ATTIO_API_BASE_URL,
) -> list[Any]:
    """Attio source factory — yields one resource per object slug plus lists+notes.

    Args:
        api_key: Attio API key (Bearer auth). Resolved from secrets by default.
        objects: standard or custom object slugs to extract records for.
        base_url: API base URL — override for testing.
    """
    client = RESTClient(base_url=base_url, auth=BearerTokenAuth(api_key))

    def _records_resource(object_slug: str) -> Any:
        @dlt.resource(
            name=object_slug,
            primary_key="record_id",
            write_disposition="replace",
            columns=_RECORD_COLUMNS.get(object_slug, columns(text=("record_id",))),
        )
        def _r() -> Iterator[Row]:
            paginator = AttioRecordCursorPaginator()
            pages = client.paginate(
                f"/v2/objects/{object_slug}/records/query",
                method="POST",
                json={},
                paginator=paginator,
                data_selector="data",
            )
            for page in skip_on_forbidden(object_slug, SCOPES_RECORDS, pages):
                for row in page:
                    nested_id = row.get("id") or {}
                    if isinstance(nested_id, dict):
                        row["record_id"] = nested_id.get("record_id")
                    promote_active_values(row)
                    yield row

        return _r

    @dlt.resource(
        name="lists",
        primary_key="list_id",
        write_disposition="replace",
        columns=_LISTS_COLUMNS,
    )
    def lists() -> Iterator[Row]:
        pages = client.paginate(
            "/v2/lists",
            paginator=SinglePagePaginator(),
            data_selector="data",
        )
        for page in skip_on_forbidden("lists", SCOPES_LISTS, pages):
            for row in page:
                nested_id = row.get("id") or {}
                if isinstance(nested_id, dict):
                    row["list_id"] = nested_id.get("list_id")
                yield row

    @dlt.resource(
        name="notes",
        primary_key="note_id",
        write_disposition="replace",
        columns=_NOTES_COLUMNS,
    )
    def notes() -> Iterator[Row]:
        pages = client.paginate(
            "/v2/notes",
            paginator=SinglePagePaginator(),
            data_selector="data",
        )
        yielded = False
        for page in skip_on_forbidden("notes", SCOPES_NOTES, pages):
            for row in page:
                nested_id = row.get("id") or {}
                if isinstance(nested_id, dict):
                    row["note_id"] = nested_id.get("note_id")
                yielded = True
                yield row
        if not yielded:
            # dlt skips writing a parquet when a resource yields zero rows
            # even with column hints + replace. Emit one all-NULL sentinel
            # so the file exists; downstream filters it via `where note_id is not null`.
            yield {"note_id": None}

    return [_records_resource(slug)() for slug in objects] + [lists, notes]


__all__ = ["attio_source"]
