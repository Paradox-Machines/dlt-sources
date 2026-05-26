"""QuickBooks Online dlt source — 24 resources matching Airbyte parity.

Auth: OAuth 2.0 refresh-token grant with *rotating* refresh tokens.
Intuit may issue a new refresh token on any refresh response; the old one
expires within ~24 h.  Handled by ``RotatingRefreshTokenAuth``.

Per-realm base path: ``/v3/company/{realmId}/...``.  All entity data is
fetched via the SQL-like query layer at ``/v3/company/{realmId}/query``,
paginated by mutating ``STARTPOSITION`` in the embedded query string.

Incremental cursor: ``MetaData.LastUpdatedTime`` (ISO-8601 with timezone)
on 18 of 24 resources.  The remaining 6 (``tax_agencies``, ``tax_rates``,
``classes``, ``departments``, ``company_info``, ``preferences``) are small
static sets fetched with ``replace`` write disposition.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from typing import Any

import dlt
import pendulum
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    QuickBooksQueryPaginator,
    RotatingRefreshTokenAuth,
    build_query,
    coerce_metadata_last_updated_time,
    columns,
)
from .settings import (
    INCREMENTAL_ENTITIES,
    MINOR_VERSION,
    QUERY_PATH_TEMPLATE,
    QUICKBOOKS_API_BASE_URL,
    QUICKBOOKS_TOKEN_URL,
    REPLACE_ENTITIES,
)

Row = dict[str, Any]

# Cursor floor: "load everything from the beginning of time".
_EPOCH_PENDULUM = pendulum.parse("1970-01-01T00:00:00Z")

# ---------------------------------------------------------------------------
# Per-resource nullable column hints.
# Declare columns referenced by downstream staging models so they persist
# in the destination schema even when all values in a load are NULL.
# ---------------------------------------------------------------------------
_INCREMENTAL_RESOURCE_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "customers": columns(
        text=(
            "display_name",
            "given_name",
            "family_name",
            "company_name",
            "primary_email_addr__address",
            "primary_phone__free_form_number",
            "bill_addr__line1",
            "bill_addr__city",
            "bill_addr__country_sub_division_code",
            "bill_addr__postal_code",
            "currency_ref__value",
        ),
    ),
    "invoices": columns(
        text=(
            "doc_number",
            "customer_ref__value",
            "customer_ref__name",
            "currency_ref__value",
            "email_status",
            "print_status",
            "private_note",
            "customer_memo__value",
        ),
    ),
    "payments": columns(
        text=(
            "customer_ref__value",
            "customer_ref__name",
            "payment_ref_num",
            "currency_ref__value",
            "private_note",
        ),
    ),
    "items": columns(
        text=(
            "name",
            "description",
            "type",
            "income_account_ref__value",
            "income_account_ref__name",
            "expense_account_ref__value",
            "expense_account_ref__name",
            "asset_account_ref__value",
            "sku",
        ),
        # QBO returns `QtyOnHand` only when `TrackQtyOnHand=true` — and
        # `UnitPrice` is similarly sparse for non-inventoried items. The
        # downstream dbt staging model (`stg_quickbooks__items.sql`)
        # `try_cast`s these to decimal; without an up-front hint dlt
        # drops the column from the parquet schema in batches that
        # contain only non-tracked items, breaking the cast at parse
        # time. Forcing nullable hints keeps the column present.
        decimal=(
            "unit_price",
            "qty_on_hand",
        ),
        boolean=(
            "track_qty_on_hand",
            "taxable",
            "active",
        ),
        timestamp=(
            "meta_data__create_time",
            "meta_data__last_updated_time",
        ),
    ),
    "accounts": columns(
        text=(
            "name",
            "fully_qualified_name",
            "account_type",
            "account_sub_type",
            "classification",
            "currency_ref__value",
        ),
    ),
    "vendors": columns(
        text=(
            "display_name",
            "given_name",
            "family_name",
            "company_name",
            "primary_email_addr__address",
            "primary_phone__free_form_number",
            "bill_addr__line1",
            "bill_addr__city",
            "bill_addr__country_sub_division_code",
            "bill_addr__postal_code",
            "currency_ref__value",
        ),
    ),
    "bills": columns(
        text=(
            "doc_number",
            "vendor_ref__value",
            "vendor_ref__name",
            "currency_ref__value",
            "private_note",
        ),
    ),
    "journal_entries": columns(
        text=(
            "doc_number",
            "currency_ref__value",
            "private_note",
        ),
    ),
}


@dlt.source(name="quickbooks")
def quickbooks_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    refresh_token: str = dlt.secrets.value,
    realm_id: str = dlt.secrets.value,
    on_token_rotation: Callable[[str], None] | None = None,
) -> list[Any]:
    """QuickBooks Online dlt source — 24 resources via the SQL-query layer.

    Args:
        client_id:         Intuit app client ID.
        client_secret:     Intuit app client secret.
        refresh_token:     Current OAuth 2.0 refresh token.
        realm_id:          Intuit company ID (shown in the QBO URL as
                           ``?company=<realmId>`` or in the app dashboard).
        on_token_rotation: Optional callback invoked with the **new** refresh
                           token whenever Intuit rotates it.  External users
                           should use this to persist the token to their own
                           secrets backend — the old token expires in ~24 h.
                           Defaults to ``None`` (a no-op is used internally).
                           See README for details.

    Returns:
        A list of dlt resource objects — 18 incremental (merge) + 6 replace.
    """
    # Default to a no-op so RotatingRefreshTokenAuth always has a callable.
    # This handles ad-hoc / REPL usage where no write-back is wired.
    if on_token_rotation is None:

        def on_token_rotation(_new: str) -> None:
            return None

    _token_url = os.environ.get("QUICKBOOKS_TOKEN_URL", QUICKBOOKS_TOKEN_URL)
    _api_base_url = os.environ.get("QUICKBOOKS_API_BASE_URL", QUICKBOOKS_API_BASE_URL)

    auth = RotatingRefreshTokenAuth(
        token_url=_token_url,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        on_token_rotation=on_token_rotation,
    )

    client = RESTClient(
        base_url=_api_base_url,
        auth=auth,
        headers={"Accept": "application/json"},
    )

    query_path = QUERY_PATH_TEMPLATE.format(realm_id=realm_id)

    # ------------------------------------------------------------------
    # Resource factory helpers (closures capture `entity` and `client`)
    # ------------------------------------------------------------------

    def _incremental_resource(entity: str, resource_name: str) -> Any:
        @dlt.resource(
            name=resource_name,
            primary_key="Id",
            write_disposition="merge",
            columns=_INCREMENTAL_RESOURCE_COLUMNS.get(resource_name, {}),
        )
        def _r(
            cursor: Any = dlt.sources.incremental(  # noqa: B008
                "MetaData.LastUpdatedTime",
                initial_value=_EPOCH_PENDULUM,
                range_start="open",
            ),
        ) -> Iterator[Row]:
            threshold = cursor.start_value  # snapshot before any yields
            start_value_iso = threshold.isoformat() if threshold else None
            base_qry = build_query(
                entity=entity,
                start_value_iso=start_value_iso,
                start_position=1,
            )
            paginator = QuickBooksQueryPaginator(entity=entity, base_query=base_qry)
            for page in client.paginate(
                query_path,
                params={"query": base_qry, "minorversion": MINOR_VERSION},
                paginator=paginator,
                data_selector=f"QueryResponse.{entity}",
            ):
                yield from page

        return _r.add_map(coerce_metadata_last_updated_time)

    def _replace_resource(entity: str, resource_name: str) -> Any:
        @dlt.resource(
            name=resource_name,
            primary_key="Id",
            write_disposition="replace",
        )
        def _r() -> Iterator[Row]:
            base_qry = build_query(entity=entity, start_value_iso=None, start_position=1)
            paginator = QuickBooksQueryPaginator(entity=entity, base_query=base_qry)
            for page in client.paginate(
                query_path,
                params={"query": base_qry, "minorversion": MINOR_VERSION},
                paginator=paginator,
                data_selector=f"QueryResponse.{entity}",
            ):
                yield from page

        return _r

    resources: list[Any] = []
    for _entity, _resource_name in INCREMENTAL_ENTITIES:
        resources.append(_incremental_resource(_entity, _resource_name))
    for _entity, _resource_name in REPLACE_ENTITIES:
        resources.append(_replace_resource(_entity, _resource_name))

    return resources


__all__ = ["quickbooks_source"]
