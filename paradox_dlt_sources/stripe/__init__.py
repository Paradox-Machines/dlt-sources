"""Stripe dlt source — charges, customers, invoices, refunds.

Public surface mirrors the Airbyte ``stripe`` connector: same resource names,
same primary keys, same field shapes after parquet schema inference.

Authentication is Bearer-token (Stripe REST API:
``Authorization: Bearer <sk_...>``).  Pagination follows Stripe's
``has_more`` + ``starting_after=<last_id>`` contract; ``created[gt]=<cursor>``
does the server-side incremental filter so each sync only fetches records
strictly newer than the prior run's last value.

``range_start="open"`` paired with ``[gt]`` ensures records exactly at
``created == last_value`` are not re-loaded on the next run — the safe
boundary for ``append``-disposition streams.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
import pendulum
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    StripeCursorPaginator,
    coerce_epoch_to_timestamp,
    columns,
    hoist_invoice_subscription,
)
from .settings import OBJECTS, STRIPE_API_BASE_URL

Row = dict[str, Any]

# Initial cursor value must be a DateTime to match what ``add_map`` coerces
# ``created`` into; int(0) raises IncrementalCursorInvalidCoercion.  The API
# still expects an epoch int for ``created[gt]`` — converted at the request site.
_EPOCH_DATETIME = pendulum.from_timestamp(0)

_CREATED_INCREMENTAL = dlt.sources.incremental(
    "created",
    initial_value=_EPOCH_DATETIME,
    range_start="open",
)

_OBJECT_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "charges": columns(
        text=(
            "customer",
            "description",
            "invoice",
            "payment_intent",
            "payment_method",
            "failure_code",
            "failure_message",
        )
    ),
    "customers": columns(
        text=(
            "email",
            "name",
            "description",
            "phone",
            "currency",
        )
    ),
    "invoices": columns(
        text=(
            "subscription",
            "customer_email",
            "customer_name",
            "number",
            "billing_reason",
            "collection_method",
            "metadata__bu",
        ),
        bigint=("due_date", "status_transitions__paid_at"),
    ),
    "refunds": columns(text=("reason",)),
}


@dlt.source(name="stripe")
def stripe_source(
    api_key: str = dlt.secrets.value,
    base_url: str = STRIPE_API_BASE_URL,
) -> list[Any]:
    """Stripe source factory — yields one resource per object.

    Args:
        api_key: Stripe secret key (``sk_...``).  Resolved from secrets by default.
        base_url: API base URL — override for testing.
    """
    client = RESTClient(
        base_url=base_url,
        auth=BearerTokenAuth(api_key),
        paginator=StripeCursorPaginator(),
    )

    def _resource(object_name: str) -> Any:
        @dlt.resource(
            name=object_name,
            primary_key="id",
            write_disposition="append",
            columns=_OBJECT_COLUMNS[object_name],
            # Stop dlt's automatic array-flattening at one level.  Without
            # this, ``invoices.lines.data[].parent…credited_items`` cascades
            # into 141-char table names that break Postgres's 63-char
            # identifier limit.  Staging models only use top-level fields
            # and the first nested array, so 1 is sufficient.
            max_table_nesting=1,
        )
        def _r(
            cursor: Any = _CREATED_INCREMENTAL,
        ) -> Iterator[Row]:
            # ``start_value`` (prior run's snapshot) — ``last_value`` would
            # shift the ``[gt]`` filter mid-iteration as we yield.
            threshold = cursor.start_value or _EPOCH_DATETIME
            params: dict[str, Any] = {
                "created[gt]": int(threshold.timestamp()),
                "limit": 100,
            }
            for page in client.paginate(f"/{object_name}", params=params):
                yield from page

        # Coerce epoch-int ``created`` to datetime so parquet infers timestamp.
        mapped = _r.add_map(coerce_epoch_to_timestamp("created"))
        if object_name == "invoices":
            mapped = mapped.add_map(hoist_invoice_subscription)
        return mapped

    return [_resource(obj) for obj in OBJECTS]


__all__ = ["stripe_source"]
