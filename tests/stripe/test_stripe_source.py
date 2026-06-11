"""Integration tests for the stripe_source factory.

Mocks Stripe HTTP endpoints with ``responses`` and asserts the source
materializes the expected resources with correct row counts and primary keys
when piped to a duckdb destination.
"""

from __future__ import annotations

import responses

from paradox_dlt_sources.stripe import stripe_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
)

_BASE = "https://api.stripe.com/v1"


def _register_stripe_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all four Stripe list endpoints."""
    # charges: two pages to exercise the StripeCursorPaginator
    rsps.add(
        responses.GET,
        f"{_BASE}/charges",
        json=load_fixture("stripe", "charges_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/charges",
        json=load_fixture("stripe", "charges_page_2"),
        status=200,
    )
    register_get(rsps, f"{_BASE}/customers", load_fixture("stripe", "customers"))
    register_get(rsps, f"{_BASE}/invoices", load_fixture("stripe", "invoices"))
    register_get(rsps, f"{_BASE}/refunds", load_fixture("stripe", "refunds"))
    register_get(
        rsps,
        f"{_BASE}/invoices/in_002/lines",
        load_fixture("stripe", "invoice_in_002_lines"),
    )


@responses.activate
def test_stripe_source_runs_against_duckdb(tmp_pipeline):
    _register_stripe_mocks(responses.mock)

    info = tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"charges", "customers", "invoices", "refunds"} <= table_names


@responses.activate
def test_charges_pagination_yields_all_rows(tmp_pipeline):
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM charges ORDER BY id")
    ids = [r[0] for r in rows]
    # 2 rows on page 1 + 1 row on page 2
    assert ids == ["ch_001", "ch_002", "ch_003"]


@responses.activate
def test_customers_rows_have_expected_fields(tmp_pipeline):
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, email, name FROM customers ORDER BY id")
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id["cus_001"] == ("alice@example.com", "Alice Smith")
    assert by_id["cus_002"] == ("bob@example.com", "Bob Jones")


@responses.activate
def test_invoice_subscription_hoist(tmp_pipeline):
    """in_002 has no top-level subscription but has one nested in parent — must be hoisted."""
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, subscription FROM invoices ORDER BY id")
    by_id = {r[0]: r[1] for r in rows}
    assert by_id["in_001"] == "sub_001"
    assert by_id["in_002"] == "sub_002"


@responses.activate
def test_created_field_coerced_to_timestamp(tmp_pipeline):
    """``created`` must be a timestamp column, not a raw integer."""
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    # dlt normalizes timestamp internally; just verify the row value is not a
    # raw epoch int (i.e. the coerce transform ran).
    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT created FROM charges WHERE id = 'ch_001'")
    assert rows, "expected at least one row"
    # The value should NOT be the raw epoch int (1700000000)
    assert rows[0][0] != 1700000000


@responses.activate
def test_refunds_resource_loads(tmp_pipeline):
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, reason FROM refunds")
    assert rows == [("re_001", "requested_by_customer")]


@responses.activate
def test_invoice_line_items_table_created(tmp_pipeline):
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "invoice_line_items" in table_names


@responses.activate
def test_invoice_line_items_stamped_and_overflow_fetched(tmp_pipeline):
    """in_001 -> 2 embedded lines; in_002 -> 1 embedded + 1 fetched overflow line."""
    _register_stripe_mocks(responses.mock)
    tmp_pipeline.run(stripe_source(api_key="sk_test_key"))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, invoice_id FROM invoice_line_items ORDER BY id")
    by_id = {r[0]: r[1] for r in rows}
    assert by_id == {
        "il_001a": "in_001",
        "il_001b": "in_001",
        "il_002a": "in_002",
        "il_002b": "in_002",
    }
