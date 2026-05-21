"""Integration tests for the agree_com_source factory.

Mocks Agree.com HTTP endpoints with ``responses`` and asserts the source
materialises the expected resources with correct row counts and primary keys
when piped to a duckdb destination.
"""

from __future__ import annotations

import responses

from paradox_dlt_sources.agree_com import agree_com_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
)

_BASE = "https://secure.agree.com"


def _register_agree_com_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all three Agree.com endpoints."""
    # agreements — two pages
    rsps.add(
        responses.GET,
        f"{_BASE}/api/v1/agreements",
        json=load_fixture("agree_com", "agreements_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/api/v1/agreements",
        json=load_fixture("agree_com", "agreements_page_2"),
        status=200,
    )
    # contacts — single page
    register_get(rsps, f"{_BASE}/api/v1/contacts", load_fixture("agree_com", "contacts_page_1"))
    # invoices — single page
    register_get(rsps, f"{_BASE}/api/v1/invoices", load_fixture("agree_com", "invoices_page_1"))


@responses.activate
def test_agree_com_source_runs_against_duckdb(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)

    info = tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"agreements", "contacts", "invoices"} <= table_names


@responses.activate
def test_agreements_has_correct_row_count(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM agreements ORDER BY id")
    # 3 rows across 2 pages
    assert len(rows) == 3
    ids = [r[0] for r in rows]
    assert ids == ["agr-001", "agr-002", "agr-003"]


@responses.activate
def test_agreements_primary_key_present(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, name FROM agreements WHERE id = 'agr-001'")
    assert len(rows) == 1
    assert rows[0][1] == "Master Services Agreement"


@responses.activate
def test_contacts_has_correct_row_count(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM contacts ORDER BY id")
    assert len(rows) == 2
    ids = [r[0] for r in rows]
    assert ids == ["con-001", "con-002"]


@responses.activate
def test_contacts_primary_key_present(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, email FROM contacts WHERE id = 'con-001'")
    assert len(rows) == 1
    assert rows[0][1] == "jane.smith@acme.com"


@responses.activate
def test_invoices_has_correct_row_count(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM invoices ORDER BY id")
    assert len(rows) == 2
    ids = [r[0] for r in rows]
    assert ids == ["inv-001", "inv-002"]


@responses.activate
def test_invoices_primary_key_present(tmp_pipeline):
    _register_agree_com_mocks(responses.mock)
    tmp_pipeline.run(agree_com_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, status FROM invoices WHERE id = 'inv-001'")
    assert len(rows) == 1
    assert rows[0][1] == "paid"
