"""Integration tests for the attio_source factory.

Mocks Attio HTTP endpoints with `responses` and asserts the source
materializes the expected resources with correct row counts and
primary keys when piped to a duckdb destination.
"""

from __future__ import annotations

import json

import responses

from paradox_dlt_sources.attio import attio_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
    register_post_sequence,
)

# The companies fixtures are 2 rows then 1 row. Driving the source at
# page_size=2 makes page 1 a "full" page, so the offset walk must fetch
# page 2 to reach `rec-c3` — the shape that regressed in PAR-1014.
_TEST_PAGE_SIZE = 2


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

    info = tmp_pipeline.run(attio_source(api_key="test-key", page_size=_TEST_PAGE_SIZE))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"companies", "people", "deals", "lists", "notes"} <= table_names


@responses.activate
def test_companies_rows_have_promoted_scalars(tmp_pipeline):
    _register_attio_mocks(responses.mock)
    tmp_pipeline.run(
        attio_source(api_key="test-key", objects=("companies",), page_size=_TEST_PAGE_SIZE)
    )

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT record_id, name FROM companies ORDER BY record_id")
    # 3 rows total across 2 pages
    assert len(rows) == 3
    by_id = {r[0]: r[1] for r in rows}
    assert by_id == {"rec-c1": "Acme Inc", "rec-c2": "Beta Corp", "rec-c3": "Gamma LLC"}


@responses.activate
def test_records_query_walks_offsets_until_short_page(tmp_pipeline):
    """PAR-1014 regression guard, asserted at the HTTP boundary.

    Every `records/query` POST must carry an explicit `limit`, and `offset`
    must climb by the page size. A single request here means the paginator
    stopped after page 1 and rows are being silently dropped.
    """
    _register_attio_mocks(responses.mock)
    tmp_pipeline.run(
        attio_source(api_key="test-key", objects=("companies",), page_size=_TEST_PAGE_SIZE)
    )

    bodies = [
        json.loads(call.request.body)
        for call in responses.calls
        if "records/query" in call.request.url
    ]
    assert bodies == [
        {"limit": 2, "offset": 0},
        {"limit": 2, "offset": 2},
    ]


@responses.activate
def test_records_query_stops_after_one_page_when_first_page_is_short(tmp_pipeline):
    """A page shorter than `limit` is the last page — don't spend a request."""
    base = "https://api.attio.com"
    register_post_sequence(
        responses.mock,
        f"{base}/v2/objects/companies/records/query",
        [load_fixture("attio", "companies_page_1")],
    )
    register_get(responses.mock, f"{base}/v2/lists", load_fixture("attio", "lists"))
    register_get(responses.mock, f"{base}/v2/notes", load_fixture("attio", "notes"))

    tmp_pipeline.run(attio_source(api_key="test-key", objects=("companies",), page_size=500))

    record_calls = [c for c in responses.calls if "records/query" in c.request.url]
    assert len(record_calls) == 1
    assert json.loads(record_calls[0].request.body) == {"limit": 500, "offset": 0}


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
