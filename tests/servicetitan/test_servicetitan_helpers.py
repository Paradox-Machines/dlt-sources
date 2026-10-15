"""Unit tests for servicetitan helpers: tenant config, auth, paginators, hints."""

from __future__ import annotations

import pytest
import requests
import responses

from paradox_dlt_sources.servicetitan import helpers
from paradox_dlt_sources.servicetitan.helpers import (
    ExportPaginator,
    HasMorePagePaginator,
    ServiceTitanAuth,
    TenantConfig,
    columns,
    make_client,
    parse_tenants,
    tag_row,
    tenant_path,
)
from paradox_dlt_sources.servicetitan.settings import (
    ENDPOINTS,
    ENDPOINTS_BY_NAME,
    PRIMARY_KEY,
    USER_AGENT,
)

_AUTH = "https://auth.servicetitan.io"
_API = "https://api.servicetitan.io"
_TENANT = {"tenant_id": 111, "name": "Interstate", "client_id": "c", "client_secret": "s"}


def test_parse_tenants_normalises_to_strings() -> None:
    [tenant] = parse_tenants([_TENANT])
    assert tenant == TenantConfig("111", "Interstate", "c", "s")


def test_tenant_repr_hides_secret() -> None:
    tenant = TenantConfig("111", "Interstate", "cid", "super-secret-value")
    assert "super-secret-value" not in repr(tenant)


@pytest.mark.parametrize(
    ("tenants", "message"),
    [
        ([], "at least one"),
        ([{**_TENANT, "client_secret": ""}], "client_secret"),
        ([_TENANT, {**_TENANT, "name": "Dup"}], "duplicate"),
    ],
)
def test_parse_tenants_rejects_bad_config(tenants: list[dict], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_tenants(tenants)


def test_endpoint_table_is_consistent() -> None:
    assert len(ENDPOINTS_BY_NAME) == len(ENDPOINTS)
    for endpoint in ENDPOINTS:
        assert endpoint.pagination in ("export", "page")
        assert endpoint.pagination == "export" or not endpoint.path.startswith("export/")
        assert not (endpoint.incremental and endpoint.pagination == "export")
    assert PRIMARY_KEY == ("tenant_id", "id")


def test_tenant_path() -> None:
    assert tenant_path(ENDPOINTS_BY_NAME["jobs"], "111") == "/jpm/v2/tenant/111/export/jobs"
    assert (
        tenant_path(ENDPOINTS_BY_NAME["form_submissions"], "222")
        == "/forms/v2/tenant/222/submissions"
    )
    assert (
        tenant_path(ENDPOINTS_BY_NAME["job_cancel_reasons"], "111")
        == "/jpm/v2/tenant/111/job-cancel-reasons"
    )


def test_tag_row_does_not_mutate_input() -> None:
    row = {"id": 1}
    tagged = tag_row(row, TenantConfig("111", "Interstate", "c", "s"))
    assert tagged == {"id": 1, "tenant_id": "111", "tenant_name": "Interstate"}
    assert row == {"id": 1}


def test_columns_are_independent_dicts() -> None:
    hints = columns(ENDPOINTS_BY_NAME["technicians"])
    assert hints["tenant_id"]["data_type"] == "text"
    assert hints["team"]["data_type"] == "text"
    assert hints["skills"]["data_type"] == "json"
    assert len({id(h) for h in hints.values()}) == len(hints)


@responses.activate
def test_auth_caches_token_and_sets_headers() -> None:
    responses.add(
        responses.POST,
        f"{_AUTH}/connect/token",
        json={"access_token": "tok-1", "expires_in": 900},
    )
    auth = ServiceTitanAuth("c", "s", "app-key", _AUTH)
    for _ in range(3):
        prepared = requests.Request("GET", f"{_API}/x").prepare()
        auth(prepared)
    assert prepared.headers["Authorization"] == "Bearer tok-1"
    assert prepared.headers["ST-App-Key"] == "app-key"
    assert len(responses.calls) == 1
    body = str(responses.calls[0].request.body)
    assert "grant_type=client_credentials" in body
    assert "client_id=c" in body
    assert "client_secret=s" in body


@responses.activate
def test_auth_sends_explicit_user_agent() -> None:
    """ServiceTitan's auth host 403s default Python user agents; both calls must override it."""
    responses.add(
        responses.POST,
        f"{_AUTH}/connect/token",
        json={"access_token": "tok-1", "expires_in": 900},
    )
    auth = ServiceTitanAuth("c", "s", "app-key", _AUTH)
    prepared = auth(requests.Request("GET", f"{_API}/x").prepare())
    token_ua = responses.calls[0].request.headers["User-Agent"]
    assert token_ua == USER_AGENT
    assert prepared.headers["User-Agent"] == USER_AGENT
    assert "python" not in USER_AGENT.lower()


@responses.activate
def test_auth_refreshes_before_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    responses.add(
        responses.POST, f"{_AUTH}/connect/token", json={"access_token": "tok-1", "expires_in": 900}
    )
    responses.add(
        responses.POST, f"{_AUTH}/connect/token", json={"access_token": "tok-2", "expires_in": 900}
    )
    now = [1_000.0]
    monkeypatch.setattr(helpers.time, "time", lambda: now[0])
    auth = ServiceTitanAuth("c", "s", "k", _AUTH)
    assert auth.access_token() == "tok-1"
    now[0] += 900 - 61
    assert auth.access_token() == "tok-1"
    now[0] += 2
    assert auth.access_token() == "tok-2"


@responses.activate
def test_auth_raises_on_bad_credentials() -> None:
    responses.add(responses.POST, f"{_AUTH}/connect/token", json={"error": "x"}, status=400)
    with pytest.raises(requests.HTTPError):
        ServiceTitanAuth("c", "bad", "k", _AUTH).access_token()


def _client() -> object:
    responses.add(
        responses.POST, f"{_AUTH}/connect/token", json={"access_token": "t", "expires_in": 900}
    )
    return make_client(ServiceTitanAuth("c", "s", "k", _AUTH), _API, max_attempts=2)


@responses.activate
def test_export_paginator_follows_continue_from() -> None:
    client = _client()
    url = f"{_API}/crm/v2/tenant/1/export/customers"
    responses.add(
        responses.GET,
        url,
        json={"hasMore": True, "continueFrom": "a", "data": [{"id": 1}]},
        match=[responses.matchers.query_param_matcher({})],
    )
    responses.add(
        responses.GET,
        url,
        json={"hasMore": False, "continueFrom": "b", "data": [{"id": 2}]},
        match=[responses.matchers.query_param_matcher({"from": "a"})],
    )
    paginator = ExportPaginator()
    pages = [list(p) for p in client.paginate(url, paginator=paginator)]  # type: ignore[attr-defined]
    assert pages == [[{"id": 1}], [{"id": 2}]]
    assert paginator.continue_from == "b"


@responses.activate
def test_has_more_paginator_stops_on_has_more_false() -> None:
    client = _client()
    url = f"{_API}/jpm/v2/tenant/1/job-types"
    for page, has_more in ((1, True), (2, True), (3, False)):
        responses.add(
            responses.GET,
            url,
            json={"hasMore": has_more, "data": [{"id": page}]},
            match=[responses.matchers.query_param_matcher({"page": str(page), "pageSize": "10"})],
        )
    pages = [list(p) for p in client.paginate(url, paginator=HasMorePagePaginator(10))]  # type: ignore[attr-defined]
    assert pages == [[{"id": 1}], [{"id": 2}], [{"id": 3}]]


@responses.activate
def test_has_more_paginator_stops_on_empty_page() -> None:
    client = _client()
    url = f"{_API}/jpm/v2/tenant/1/job-types"
    responses.add(responses.GET, url, json={"hasMore": True, "data": []})
    pages = [list(p) for p in client.paginate(url, paginator=HasMorePagePaginator(10))]  # type: ignore[attr-defined]
    assert pages == [[]]
