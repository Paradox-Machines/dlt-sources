"""
Apollo.io dlt source — extracts CRM and prospecting data from the Apollo.io REST API.

Resources:
  - contacts        (CRM contacts, incremental by updated_at)
  - accounts        (CRM accounts/organizations, full replace)
  - people          (Apollo prospecting database, full replace — NOTE: consumes API credits)
  - opportunities   (CRM deals/opportunities, full replace)
  - sequences       (Email sequences/campaigns, full replace)
  - users           (Workspace users, full replace)
  - email_accounts  (Connected email accounts, full replace)
  - labels          (Contact/account labels, full replace)

Auth: API key passed as x-api-key header (api_key_header auth).

Pagination: Page-number based (page + per_page params) for all search endpoints.

OPEN QUESTIONS (from research artifact):
  - Auth method: docs mention both ?api_key= query param AND x-api-key header.
  - Exact max per_page limit and total_pages field name need confirmation.
  - Incremental filter: sort_by_field=contact_updated_at approximates recency
    but a true updated_at[gt] filter has NOT been confirmed.
  - People/mixed_people endpoint consumes credits per row — confirm credit costs.
  - Opportunities endpoint path: /opportunities/search vs. /deals/search.
  - Rate limits: plan-dependent, exact limits not captured.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from functools import wraps
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import ApolloIoPagePaginator
from .settings import (
    APOLLO_IO_API_BASE_URL,
    DEFAULT_PER_PAGE,
    ENDPOINT_ACCOUNTS_SEARCH,
    ENDPOINT_CONTACTS_SEARCH,
    ENDPOINT_EMAIL_ACCOUNTS,
    ENDPOINT_LABELS,
    ENDPOINT_OPPORTUNITIES,
    ENDPOINT_PEOPLE_SEARCH,
    ENDPOINT_SEQUENCES,
    ENDPOINT_USERS,
)

Row = dict[str, Any]

_CONTACTS_INCREMENTAL: dlt.sources.incremental[str] = dlt.sources.incremental(
    "updated_at",
    initial_value=None,
    on_cursor_value_missing="include",
)


def _sentinel_backed(
    *pk_columns: str,
    _types: dict[str, str] | None = None,
) -> Callable[[Callable[..., Iterator[Any]]], Callable[..., Iterator[Any]]]:
    """Wrap a resource generator so an empty resource still emits one all-NULL row.

    Applied by the harness directly BELOW ``@dlt.resource``. Drives the inner
    generator; if it yields zero rows, yields a single all-NULL sentinel row over
    the sentinel column(s) so dlt still writes the table (the attio pattern). The
    sentinel is all-NULL, so downstream / the harness post-gate treats it as a
    sentinel, NOT real data.

    The all-NULL sentinel row carries PER-ITEM type hints (``dlt.mark.with_hints``
    / ``dlt.mark.make_hints``) so EACH sentinel column materializes WITH a type
    even though every value is None — without the hint dlt cannot infer a type for
    an all-NULL column and DROPS it ("...columns ... could not have their types
    inferred ... will not be materialized"), leaving the empty resource's table
    column-less. The hint rides on the SENTINEL ITEM, so it is emitted ONLY when
    the inner generator yielded nothing (empty fixture / empty production window):
    a real-data load never takes this branch, so dlt's native type inference still
    governs (an integer PK stays BIGINT — no static-``data_type`` poison). The hint
    adds TYPES, never VALUES — the row is still all-NULL, so it stays classified as
    a sentinel. ``_types`` maps a sentinel column to its declared dlt ``data_type``
    (from the model's ``@dlt.resource(columns=...)``); an undeclared column defaults
    to ``"text"`` (safe — only ever applied to an EMPTY resource, never to data).
    """
    columns = pk_columns or ("id",)
    types = _types or {}

    def _decorate(fn: Callable[..., Iterator[Any]]) -> Callable[..., Iterator[Any]]:
        @wraps(fn)
        def _wrapped(*args: Any, **kwargs: Any) -> Iterator[Any]:
            yielded = False
            for row in fn(*args, **kwargs):
                yielded = True
                yield row
            if not yielded:
                sentinel = {column: None for column in columns}
                hints = dlt.mark.make_hints(
                    columns={
                        column: {
                            "data_type": types.get(column, "text"),
                            "nullable": True,
                        }
                        for column in columns
                    }
                )
                yield dlt.mark.with_hints(sentinel, hints)

        return _wrapped

    return _decorate


def _nullable_pk(*pk_columns: str) -> Callable[[Any], Any]:
    """Relax a resource's sentinel column(s) to nullable so the sentinel passes.

    Applied by the harness directly ABOVE ``@dlt.resource``. A dlt primary key is
    non-nullable by default, so an all-NULL sentinel row would raise
    ``UnboundColumnException`` at normalize; a non-nullable DECLARED column (or the
    incremental cursor field) would likewise raise a NOT NULL ConstraintException.
    This hints each named column ``nullable=True`` ONLY — it does NOT pin a
    ``data_type``, so the column keeps its NATIVE inferred type when real rows
    arrive (F-TYPE: the old ``data_type="text"`` poisoned every PK to text). It only
    PERMITS NULL — inert when real rows arrive.
    """

    def _decorate(resource: Any) -> Any:
        if pk_columns:
            resource.apply_hints(columns={column: {"nullable": True} for column in pk_columns})
        return resource

    return _decorate


def _sentinel_incremental(cursor: str, **kwargs: Any) -> Callable[[Any], Any]:
    """Re-apply an incremental that INCLUDES a missing/NULL cursor on an empty resource.

    Applied by the harness directly ABOVE ``@dlt.resource`` for an INCREMENTAL
    resource. The harness sentinel row is all-NULL and omits the cursor field, so
    dlt's default incremental (``on_cursor_value_missing="raise"``) would raise
    ``IncrementalCursorPathMissing`` for the empty resource. This builds a FRESH
    ``dlt.sources.incremental`` over the same cursor + preserved params with
    ``on_cursor_value_missing="include"`` and re-applies it via ``apply_hints`` so
    the sentinel row is included (the table still materializes) while real rows are
    filtered by the cursor exactly as before — inert when real rows arrive.
    """

    def _decorate(resource: Any) -> Any:
        resource.apply_hints(
            incremental=dlt.sources.incremental(cursor, on_cursor_value_missing="include", **kwargs)
        )
        return resource

    return _decorate


def _build_contacts(
    client: RESTClient,
    updated_at: dlt.sources.incremental[str],
) -> Any:
    @_nullable_pk("id")
    @dlt.resource(
        name="contacts",
        primary_key="id",
        write_disposition="append",
    )
    @_sentinel_backed("id")
    def contacts(
        updated_at: dlt.sources.incremental[str] = updated_at,
    ) -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
                "sort_by_field": "contact_updated_at",
                "sort_ascending": False,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_CONTACTS_SEARCH,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("contacts") or []
            if not rows:
                break
            start_val = updated_at.start_value
            exhausted = False
            for row in rows:
                row_updated = row.get("updated_at") or ""
                if start_val and row_updated and row_updated < start_val:
                    exhausted = True
                    break
                yield row
            if exhausted or len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return contacts


def _build_accounts(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="accounts", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def accounts() -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_ACCOUNTS_SEARCH,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("accounts") or []
            if not rows:
                break
            yield from rows
            if len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return accounts


def _build_people(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="people", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def people() -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_PEOPLE_SEARCH,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("people") or []
            if not rows:
                break
            yield from rows
            if len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return people


def _build_opportunities(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(
        name="opportunities",
        primary_key="id",
        write_disposition="replace",
    )
    @_sentinel_backed("id")
    def opportunities() -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_OPPORTUNITIES,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("opportunities") or []
            if not rows:
                break
            yield from rows
            if len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return opportunities


def _build_sequences(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="sequences", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def sequences() -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_SEQUENCES,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("emailer_campaigns") or []
            if not rows:
                break
            yield from rows
            if len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return sequences


def _build_users(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def users() -> Iterator[Row]:
        page_num = 1
        while True:
            body: dict[str, Any] = {
                "per_page": DEFAULT_PER_PAGE,
                "page": page_num,
            }
            response = client.session.post(
                client.base_url.rstrip("/") + ENDPOINT_USERS,
                json=body,
            )
            response.raise_for_status()
            data = response.json()
            rows: list[Row] = data.get("users") or []
            if not rows:
                break
            yield from rows
            if len(rows) < DEFAULT_PER_PAGE:
                break
            page_num += 1

    return users


def _build_email_accounts(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(
        name="email_accounts",
        primary_key="id",
        write_disposition="replace",
    )
    @_sentinel_backed("id")
    def email_accounts() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_EMAIL_ACCOUNTS,
            paginator=ApolloIoPagePaginator(),
            data_selector="email_accounts",
        ):
            yield from page

    return email_accounts


def _build_labels(client: RESTClient) -> Any:
    @_nullable_pk("id")
    @dlt.resource(name="labels", primary_key="id", write_disposition="replace")
    @_sentinel_backed("id")
    def labels() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_LABELS,
            paginator=ApolloIoPagePaginator(),
            data_selector="labels",
        ):
            yield from page

    return labels


@dlt.source(name="apollo_io")
def apollo_io_source(
    api_key: str = dlt.secrets.value,
    base_url: str = APOLLO_IO_API_BASE_URL,
) -> list[Any]:
    """apollo_io source factory.

    Args:
        api_key: API token. Resolved from secrets by default.
        base_url: Test seam — pass the full pre-built base URL to bypass default
            construction (avoids env-var leakage between parallel tests).
            Production callers leave this as the settings default.
    """
    client = RESTClient(
        base_url=base_url,
        auth=APIKeyAuth(name="X-Api-Key", api_key=api_key, location="header"),
        headers={"Accept": "application/json"},
    )

    contacts = _build_contacts(client, _CONTACTS_INCREMENTAL)
    accounts = _build_accounts(client)
    people = _build_people(client)
    opportunities = _build_opportunities(client)
    sequences = _build_sequences(client)
    users = _build_users(client)
    email_accounts = _build_email_accounts(client)
    labels = _build_labels(client)

    return [contacts, accounts, people, opportunities, sequences, users, email_accounts, labels]


__all__ = ["apollo_io_source"]
