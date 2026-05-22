"""Agree.com dlt source — agreements, contacts, invoices.

Auth: Bearer token (API key) passed as ``api_key``.

Pagination: ``PageNumberPaginator`` against the Agree.com v1 list endpoints.
Response shape is ``{data, pagination}`` with ``pagination.total_pages``.

Incremental cursors:
  * ``contacts`` carries a real ``updated_at`` (verified live) and uses an
    ``updated_at`` incremental cursor with ``write_disposition="append"``.
  * ``agreements`` and ``invoices`` do NOT expose any monotonic last-modified
    timestamp, so both use ``write_disposition="replace"`` (full snapshot per
    run).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import EPOCH_ISO, agree_paginator, columns
from .settings import (
    AGREE_API_BASE_URL,
    DEFAULT_PAGE_SIZE,
    ENDPOINT_AGREEMENTS,
    ENDPOINT_CONTACTS,
    ENDPOINT_INVOICES,
)

Row = dict[str, Any]

# ── Column hints ──────────────────────────────────────────────────────────────
# Explicit hints prevent dlt from silently dropping all-NULL columns, which
# would cause downstream dbt staging models to fail when they reference those
# columns. Nested-dict fields arrive after dlt normalization as flattened
# names, e.g. ``amount.amount`` → ``amount__amount``.

_AGREEMENTS_COLUMNS = columns(
    text=(
        "id",
        "name",
        "status",
        "organization_id",
        "invoice_template_id",
        "delivery_mode",
        "reminder_schedule",
        "docs_url",
        "share_url",
        "preview_url",
    ),
    bigint=("version",),
    boolean=(
        "payments_enabled",
        "signing_order_enabled",
        "forward_signature_enabled",
    ),
    timestamp=(
        "starts_at",
        "executed_at",
        "ends_at",
        "deleted_at",
        "reminder_scheduled_at",
        "last_reminder_sent_at",
    ),
)

_CONTACTS_COLUMNS = columns(
    text=(
        "id",
        "name",
        "email",
        "title",
        "company",
        "organization_id",
    ),
    timestamp=(
        "inserted_at",
        "updated_at",
    ),
)

_INVOICES_COLUMNS = columns(
    text=(
        "id",
        "name",
        "agreement_id",
        "status",
        "amount__currency",
        "billing_contact__email",
        "billing_contact__name",
        "billing_contact__company",
        "customer_id",
        "organization_id",
        "destination_organization_id",
        "external_id",
        "external_customer_id",
        "qbo_invoice_id",
        "xero_invoice_id",
        "delivery_method",
        "payment_type",
        "memo",
        "invoice_url",
        "payment_link",
        "subscription_url",
    ),
    bigint=(
        "amount__amount",
        "recurring_sequence",
    ),
    boolean=("automatic_delivery",),
    decimal=("sales_tax_percentage",),
    timestamp=(
        "inserted_at",
        "due_at",
        "paid_at",
        "sent_at",
        "reviewed_at",
        "authorized_at",
        "processing_at",
        "scheduled_at",
        "reminder_scheduled_at",
        "last_reminder_sent_at",
    ),
)


@dlt.source(name="agree_com")
def agree_com_source(
    api_key: str = dlt.secrets.value,
    base_url: str = AGREE_API_BASE_URL,
) -> list[Any]:
    """Agree.com source factory — yields agreements, contacts, and invoices.

    Args:
        api_key: Agree.com API key (Bearer auth). Resolved from secrets by default.
        base_url: API base URL — override for testing.
    """
    client = RESTClient(base_url=base_url, auth=BearerTokenAuth(api_key))

    @dlt.resource(
        name="agreements",
        primary_key="id",
        write_disposition="replace",
        columns=_AGREEMENTS_COLUMNS,
    )
    def agreements() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_AGREEMENTS,
            params={"page_size": DEFAULT_PAGE_SIZE},
            paginator=agree_paginator(),
            data_selector="data",
        ):
            yield from page

    @dlt.resource(
        name="contacts",
        primary_key="id",
        write_disposition="append",
        columns=_CONTACTS_COLUMNS,
    )
    def contacts(
        cursor: dlt.sources.incremental[str] = dlt.sources.incremental(  # noqa: B008  # type: ignore[assignment]
            "updated_at",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_CONTACTS,
            params={"page_size": DEFAULT_PAGE_SIZE},
            paginator=agree_paginator(),
            data_selector="data",
        ):
            yield from page

    @dlt.resource(
        name="invoices",
        primary_key="id",
        write_disposition="replace",
        columns=_INVOICES_COLUMNS,
    )
    def invoices() -> Iterator[Row]:
        for page in client.paginate(
            ENDPOINT_INVOICES,
            params={"page_size": DEFAULT_PAGE_SIZE},
            paginator=agree_paginator(),
            data_selector="data",
        ):
            yield from page

    return [agreements, contacts, invoices]


__all__ = ["agree_com_source"]
