"""Integration tests for the hubspot_source factory.

Mocks HubSpot HTTP endpoints with ``responses`` and asserts the source
materializes the expected resources with correct row counts and shapes
when piped to a duckdb destination.
"""

from __future__ import annotations

import json

import responses

from paradox_dlt_sources.hubspot import hubspot_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
)

_BASE = "https://api.hubapi.com"


def _register_all_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned GET responses for all 5 HubSpot endpoints."""
    # CRM v3 objects — each uses GET with cursor pagination
    rsps.add(
        responses.GET,
        f"{_BASE}/crm/v3/objects/companies",
        json=load_fixture("hubspot", "companies_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/crm/v3/objects/companies",
        json=load_fixture("hubspot", "companies_page_2"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/crm/v3/objects/contacts",
        json=load_fixture("hubspot", "contacts_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/crm/v3/objects/deals",
        json=load_fixture("hubspot", "deals_page_1"),
        status=200,
    )
    # Engagements v1 — two pages
    rsps.add(
        responses.GET,
        f"{_BASE}/engagements/v1/engagements/paged",
        json=load_fixture("hubspot", "engagements_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/engagements/v1/engagements/paged",
        json=load_fixture("hubspot", "engagements_page_2"),
        status=200,
    )
    # Deal pipelines — single page
    register_get(rsps, f"{_BASE}/crm/v3/pipelines/deals", load_fixture("hubspot", "deal_pipelines"))


@responses.activate
def test_hubspot_source_runs_against_duckdb(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)

    info = tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"companies", "contacts", "deals", "engagements", "deal_pipelines"} <= table_names


@responses.activate
def test_companies_pagination_yields_all_pages(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM companies ORDER BY id")
    ids = [r[0] for r in rows]
    # 2 rows page-1 + 1 row page-2
    assert len(ids) == 3
    assert set(ids) == {"co-1", "co-2", "co-3"}


@responses.activate
def test_contacts_resource_loaded(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM contacts")
    assert len(rows) == 1
    assert rows[0][0] == "ct-1"


@responses.activate
def test_deals_resource_loaded(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM deals")
    assert len(rows) == 1
    assert rows[0][0] == "deal-1"


@responses.activate
def test_engagements_flattened_across_two_pages(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, type, associations__contact_ids FROM engagements ORDER BY id"
        )
    assert len(rows) == 3
    ids = {r[0] for r in rows}
    assert ids == {101, 102, 103}

    # Verify association IDs serialized as JSON strings
    row_101 = next(r for r in rows if r[0] == 101)
    assert json.loads(row_101[2]) == [1001, 1002]


@responses.activate
def test_deal_pipelines_stages_serialized_as_json(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, stages FROM deal_pipelines ORDER BY id")
    assert len(rows) == 2

    pipeline_ids = {r[0] for r in rows}
    assert pipeline_ids == {"pipeline-1", "pipeline-2"}

    # stages column must be a JSON string, not a child table
    for _, stages_val in rows:
        parsed = json.loads(stages_val)
        assert isinstance(parsed, list)
        assert len(parsed) > 0


@responses.activate
def test_companies_properties_columns_present(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(hubspot_source(api_key="test-key", base_url=_BASE))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, properties__name, properties__domain FROM companies ORDER BY id"
        )
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id["co-1"] == ("Acme Corp", "acme.com")
    assert by_id["co-2"] == ("Beta Inc", "beta.io")
    assert by_id["co-3"] == ("Gamma LLC", "gamma.co")
