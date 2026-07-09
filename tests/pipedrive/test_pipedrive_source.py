"""Integration tests for the pipedrive_source factory.

Mocks Pipedrive HTTP endpoints with `responses` and asserts the source
materializes the expected resources with correct row counts and field values
when piped to a duckdb destination.
"""

from __future__ import annotations

import responses

from paradox_dlt_sources.pipedrive import pipedrive_source
from tests._helpers.fixture_loader import load_fixture, register_get

BASE = "https://api.pipedrive.com/v1"


def _register_all_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all 7 pipedrive endpoints."""
    # /recents (users)
    register_get(rsps, f"{BASE}/recents", load_fixture("pipedrive", "users_recents"))
    # /persons — two pages
    register_get(rsps, f"{BASE}/persons", load_fixture("pipedrive", "persons_page_1"))
    register_get(rsps, f"{BASE}/persons", load_fixture("pipedrive", "persons_page_2"))
    # /leads
    register_get(rsps, f"{BASE}/leads", load_fixture("pipedrive", "leads"))
    # /organizations
    register_get(rsps, f"{BASE}/organizations", load_fixture("pipedrive", "organizations"))
    # /deals
    register_get(rsps, f"{BASE}/deals", load_fixture("pipedrive", "deals"))
    # /activities
    register_get(rsps, f"{BASE}/activities", load_fixture("pipedrive", "activities"))
    # /stages
    register_get(rsps, f"{BASE}/stages", load_fixture("pipedrive", "stages"))


@responses.activate
def test_pipedrive_source_runs_against_duckdb(tmp_pipeline: object) -> None:
    _register_all_mocks(responses.mock)

    info = tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}  # type: ignore[attr-defined]
    expected = {"users", "persons", "leads", "organizations", "deals", "activities", "stages"}
    assert expected <= table_names


@responses.activate
def test_persons_rows_have_flattened_contact_fields(tmp_pipeline: object) -> None:
    """email_primary / phone_primary must be lifted from the nested arrays."""
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(  # type: ignore[attr-defined]
        pipedrive_source(api_key="test-key", base_url=BASE)
    )

    with tmp_pipeline.sql_client() as client:  # type: ignore[attr-defined]
        rows = client.execute_sql(
            "SELECT id, email_primary, phone_primary FROM persons ORDER BY id"
        )
    assert len(rows) == 3
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id[1] == ("alice@acme.com", "+15551001001")
    assert by_id[2] == ("bob@beta.com", None)
    assert by_id[3] == ("carol@gamma.com", "+15553003003")


@responses.activate
def test_persons_owner_id_flattened_to_scalar(tmp_pipeline: object) -> None:
    """`owner_id` nested user object must be flattened to its scalar `.id`."""
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    with tmp_pipeline.sql_client() as client:  # type: ignore[attr-defined]
        rows = client.execute_sql("SELECT id, owner_id FROM persons ORDER BY id")
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[1] == 10
    assert by_id[2] == 11


@responses.activate
def test_deals_user_refs_flattened_to_scalar(tmp_pipeline: object) -> None:
    """`user_id` and `creator_user_id` nested objects must flatten to scalar ids."""
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    with tmp_pipeline.sql_client() as client:  # type: ignore[attr-defined]
        rows = client.execute_sql("SELECT id, user_id, creator_user_id FROM deals")
    assert len(rows) == 1
    assert rows[0] == (101, 10, 10)


@responses.activate
def test_users_resource_unwraps_recents_envelope(tmp_pipeline: object) -> None:
    """Users must be extracted from the `data` field inside each recents envelope."""
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    with tmp_pipeline.sql_client() as client:  # type: ignore[attr-defined]
        rows = client.execute_sql("SELECT id, name FROM users ORDER BY id")
    assert len(rows) == 2
    by_id = {r[0]: r[1] for r in rows}
    assert by_id[10] == "Owner A"
    assert by_id[11] == "Owner B"


@responses.activate
def test_api_token_never_appears_in_request_url(tmp_pipeline: object) -> None:
    """The personal API token must ride in the `x-api-token` header, never the
    query string.

    Pipedrive v1 historically accepted `?api_token=<value>`, but requests bakes
    the full `response.url` (query string included) into every `HTTPError`
    message. dlt wraps that in `ResourceExtractionError`, so a query-string
    token leaks into Dagster/stdout logs on any 4xx/5xx. Header auth keeps the
    secret out of the URL entirely — the only reliable fix for that leak.
    """
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    pipedrive_calls = [c for c in responses.calls if c.request.url.startswith(BASE)]
    assert pipedrive_calls, "expected at least one request to Pipedrive"
    for call in pipedrive_calls:
        assert "api_token" not in call.request.url, f"token leaked into URL: {call.request.url}"
        assert call.request.headers.get("x-api-token") == "test-key", (
            f"expected x-api-token header on {call.request.url}"
        )


@responses.activate
def test_stages_resource_uses_replace_disposition(tmp_pipeline: object) -> None:
    """Stages uses replace write disposition — full snapshot every run."""
    _register_all_mocks(responses.mock)
    tmp_pipeline.run(pipedrive_source(api_key="test-key", base_url=BASE))  # type: ignore[attr-defined]

    with tmp_pipeline.sql_client() as client:  # type: ignore[attr-defined]
        rows = client.execute_sql("SELECT id, name, pipeline_name FROM stages ORDER BY id")
    assert len(rows) == 2
    assert rows[0] == (1, "Qualified", "Sales Pipeline")
    assert rows[1] == (5, "Proposal", "Sales Pipeline")
