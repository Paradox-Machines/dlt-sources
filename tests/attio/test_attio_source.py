"""Integration tests for the attio_source factory.

Mocks Attio HTTP endpoints with `responses` and asserts the source
materializes the expected resources with correct row counts and
primary keys when piped to a duckdb destination.
"""

from __future__ import annotations

import responses

from paradox_dlt_sources.attio import attio_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
    register_post_sequence,
)


def _register_attio_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all 5 attio endpoints."""
    base = "https://api.attio.com"
    for slug in ("companies", "people", "deals"):
        # All three records endpoints use the same companies fixtures for
        # simplicity — the source code paths are identical per slug.
        register_post_sequence(
            rsps,
            f"{base}/v2/objects/{slug}/records/query",
            [load_fixture("attio", "companies_page_1"), load_fixture("attio", "companies_page_2")],
        )
    register_get(rsps, f"{base}/v2/lists", load_fixture("attio", "lists"))
    register_get(rsps, f"{base}/v2/notes", load_fixture("attio", "notes"))


@responses.activate
def test_attio_source_runs_against_duckdb(tmp_pipeline):
    _register_attio_mocks(responses.mock)

    info = tmp_pipeline.run(attio_source(api_key="test-key"))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"companies", "people", "deals", "lists", "notes"} <= table_names


@responses.activate
def test_companies_rows_have_promoted_scalars(tmp_pipeline):
    _register_attio_mocks(responses.mock)
    tmp_pipeline.run(attio_source(api_key="test-key", objects=("companies",)))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT record_id, name FROM companies ORDER BY record_id")
    # 3 rows total across 2 pages
    assert len(rows) == 3
    by_id = {r[0]: r[1] for r in rows}
    assert by_id == {"rec-c1": "Acme Inc", "rec-c2": "Beta Corp", "rec-c3": "Gamma LLC"}


@responses.activate
def test_lists_resource_uses_single_page(tmp_pipeline):
    _register_attio_mocks(responses.mock)
    tmp_pipeline.run(attio_source(api_key="test-key", objects=()))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT list_id, name FROM lists")
    assert rows == [("list-1", "Key Accounts")]


@responses.activate
def test_notes_resource_writes_data(tmp_pipeline):
    _register_attio_mocks(responses.mock)
    tmp_pipeline.run(attio_source(api_key="test-key", objects=()))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT note_id, title FROM notes")
    assert ("note-1", "Kickoff call") in rows


@responses.activate
def test_notes_emits_sentinel_when_zero_rows(tmp_pipeline):
    base = "https://api.attio.com"
    register_get(responses.mock, f"{base}/v2/notes", {"data": []})
    register_get(responses.mock, f"{base}/v2/lists", {"data": []})

    tmp_pipeline.run(attio_source(api_key="test-key", objects=()))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT note_id FROM notes")
    # One sentinel row with NULL note_id so the table exists
    assert rows == [(None,)]
