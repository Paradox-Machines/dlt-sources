"""Integration tests for quickbooks_source factory.

Mocks QBO HTTP endpoints with `responses` and asserts the source
materialises the expected resources with correct row counts when piped
to a duckdb destination.

The token endpoint is mocked so no real Intuit credentials are needed.
"""

from __future__ import annotations

import responses as responses_lib

from paradox_dlt_sources.quickbooks import quickbooks_source
from tests._helpers.fixture_loader import load_fixture

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
API_BASE = "https://quickbooks.api.intuit.com"
REALM_ID = "test-realm-123"
QUERY_URL = f"{API_BASE}/v3/company/{REALM_ID}/query"

_TOKEN_BODY = {
    "access_token": "test-access-token",
    "expires_in": 3600,
    "refresh_token": "test-refresh-token",
}


def _register_empty_for(rsps: responses_lib.RequestsMock, url: str) -> None:
    """Register an empty QueryResponse for an entity at *url*."""
    rsps.add(
        responses_lib.GET,
        url,
        json={"QueryResponse": {}},
        status=200,
        match_querystring=False,
    )


@responses_lib.activate
def test_quickbooks_source_invoices_single_page(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """invoices resource loads rows correctly (single short page, no second call)."""
    # Token endpoint (one call per run; access token is cached)
    responses_lib.add(responses_lib.POST, TOKEN_URL, json=_TOKEN_BODY, status=200)

    # Entities are iterated in settings order:
    #   index 0: customers, index 1: invoices, indices 2–23: others
    # Register one empty for customers, then the invoices fixture, then 22 empties.
    _register_empty_for(responses_lib.mock, QUERY_URL)  # customers (index 0)
    responses_lib.add(
        responses_lib.GET,
        QUERY_URL,
        json=load_fixture("quickbooks", "invoices_page_1"),
        status=200,
        match_querystring=False,
    )
    for _ in range(22):  # payments … preferences (indices 2–23)
        _register_empty_for(responses_lib.mock, QUERY_URL)

    info = tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="rtoken",
            realm_id=REALM_ID,
        )
    )

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "invoices" in table_names

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM invoices ORDER BY id")
    assert len(rows) == 2
    ids = {r[0] for r in rows}
    assert ids == {"1", "2"}


@responses_lib.activate
def test_quickbooks_source_on_token_rotation_callback(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """on_token_rotation callback fires when Intuit rotates the refresh token."""
    rotated: list[str] = []

    # Token endpoint returns a NEW refresh token
    responses_lib.add(
        responses_lib.POST,
        TOKEN_URL,
        json={
            "access_token": "tok-new",
            "expires_in": 3600,
            "refresh_token": "rotated-refresh-XYZ",
        },
        status=200,
    )

    # All 24 entity queries return empty
    for _ in range(24):
        _register_empty_for(responses_lib.mock, QUERY_URL)

    tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="old-refresh-token",
            realm_id=REALM_ID,
            on_token_rotation=rotated.append,
        )
    )

    # Callback must have fired with the new token
    assert rotated == ["rotated-refresh-XYZ"]


@responses_lib.activate
def test_quickbooks_source_no_rotation_callback_when_token_unchanged(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """on_token_rotation is NOT called when the refresh token is unchanged."""
    rotated: list[str] = []

    responses_lib.add(
        responses_lib.POST,
        TOKEN_URL,
        json={
            "access_token": "tok",
            "expires_in": 3600,
            "refresh_token": "same-token",  # same as initial
        },
        status=200,
    )

    for _ in range(24):
        _register_empty_for(responses_lib.mock, QUERY_URL)

    tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="same-token",
            realm_id=REALM_ID,
            on_token_rotation=rotated.append,
        )
    )

    assert rotated == []


@responses_lib.activate
def test_quickbooks_source_no_op_when_callback_is_none(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """Source runs successfully when on_token_rotation=None (default no-op)."""
    responses_lib.add(
        responses_lib.POST,
        TOKEN_URL,
        json={"access_token": "tok", "expires_in": 3600, "refresh_token": "rotated-new"},
        status=200,
    )

    for _ in range(24):
        _register_empty_for(responses_lib.mock, QUERY_URL)

    info = tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="old-token",
            realm_id=REALM_ID,
            on_token_rotation=None,  # explicit None
        )
    )
    # No exception — no-op worked
    assert not info.has_failed_jobs


@responses_lib.activate
def test_quickbooks_source_tax_agencies_replace_resource(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """tax_agencies (replace resource) is loaded from the fixture."""
    responses_lib.add(responses_lib.POST, TOKEN_URL, json=_TOKEN_BODY, status=200)

    # Entity order: 18 incremental + 6 replace. tax_agencies is index 18 (0-based).
    for _ in range(18):
        _register_empty_for(responses_lib.mock, QUERY_URL)
    # tax_agencies
    responses_lib.add(
        responses_lib.GET,
        QUERY_URL,
        json=load_fixture("quickbooks", "tax_agencies"),
        status=200,
        match_querystring=False,
    )
    # remaining 5 replace entities
    for _ in range(5):
        _register_empty_for(responses_lib.mock, QUERY_URL)

    info = tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="rtoken",
            realm_id=REALM_ID,
        )
    )

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "tax_agencies" in table_names

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM tax_agencies")
    assert len(rows) == 1
    assert rows[0][0] == "1"


@responses_lib.activate
def test_quickbooks_source_realm_id_appears_in_query_path(tmp_pipeline) -> None:  # type: ignore[no-untyped-def]
    """realm_id is embedded in the API request path, not as a query param."""
    specific_realm = "my-specific-realm-456"
    specific_url = f"{API_BASE}/v3/company/{specific_realm}/query"

    responses_lib.add(responses_lib.POST, TOKEN_URL, json=_TOKEN_BODY, status=200)
    # Register enough empty responses at the realm-specific URL
    for _ in range(24):
        _register_empty_for(responses_lib.mock, specific_url)

    info = tmp_pipeline.run(
        quickbooks_source(
            client_id="cid",
            client_secret="csec",
            refresh_token="rtoken",
            realm_id=specific_realm,
        )
    )

    assert not info.has_failed_jobs
    # All calls (except the token call) should use the realm-specific path
    api_calls = [c for c in responses_lib.calls if "company" in (c.request.url or "")]
    for c in api_calls:
        assert specific_realm in (c.request.url or "")
