"""Integration tests for servicetitan_source against a DuckDB destination.

All ServiceTitan auth + API HTTP is mocked with ``responses`` (never
pytest-httpx, per CONTRIBUTING.md).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
import responses
from responses import matchers

from paradox_dlt_sources.servicetitan import servicetitan_source
from tests._helpers.fixture_loader import load_fixture

_API = "https://api.servicetitan.io"
_AUTH = "https://auth.servicetitan.io"
_TOKEN_URL = f"{_AUTH}/connect/token"

_INTERSTATE = {
    "tenant_id": "111",
    "name": "Interstate",
    "client_id": "cid-111",
    "client_secret": "secret-111",
}
_OSC = {
    "tenant_id": "222",
    "name": "OSC",
    "client_id": "cid-222",
    "client_secret": "secret-222",
}


def _url(tenant: dict[str, str], module: str, path: str) -> str:
    return f"{_API}/{module}/v2/tenant/{tenant['tenant_id']}/{path}"


def _source(tenants: list[dict[str, str]], resources: list[str], **kwargs: Any) -> Any:
    return servicetitan_source(
        tenants=tenants,
        app_key="app-key",
        resources=resources,
        api_base_url=_API,
        auth_base_url=_AUTH,
        **kwargs,
    )


def _register_token(rsps: responses.RequestsMock) -> None:
    rsps.add(responses.POST, _TOKEN_URL, json=load_fixture("servicetitan", "token_response"))


def _export_body(rows: list[dict[str, Any]], token: str = "done", has_more: bool = False) -> dict:
    return {"hasMore": has_more, "continueFrom": token, "data": rows}


def _query(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


def _api_calls(rsps: responses.RequestsMock, url: str) -> list[Any]:
    return [c for c in rsps.calls if c.request.url.split("?")[0] == url]


def test_two_tenants_same_id_do_not_collide(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        for tenant, number in ((_INTERSTATE, "IAC-1"), (_OSC, "OSC-1")):
            rsps.add(
                responses.GET,
                _url(tenant, "jpm", "export/jobs"),
                json=_export_body(
                    [{"id": 42, "jobNumber": number, "modifiedOn": "2024-03-01T00:00:00Z"}]
                ),
            )
        tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["jobs"]))

        api_requests = [c.request for c in rsps.calls if c.request.url.startswith(_API)]
        assert api_requests
        for request in api_requests:
            assert request.headers["ST-App-Key"] == "app-key"
            assert request.headers["Authorization"] == "Bearer test-access-token"
        token_bodies = sorted(
            str(c.request.body) for c in rsps.calls if c.request.url == _TOKEN_URL
        )
        assert len(token_bodies) == 2
        assert "client_id=cid-111" in token_bodies[0]
        assert "client_id=cid-222" in token_bodies[1]

    rows = _query(
        tmp_pipeline, "SELECT tenant_id, tenant_name, id, job_number FROM jobs ORDER BY tenant_id"
    )
    assert rows == [("111", "Interstate", 42, "IAC-1"), ("222", "OSC", 42, "OSC-1")]
    pk = [
        name
        for name, col in tmp_pipeline.default_schema.get_table_columns("jobs").items()
        if col.get("primary_key")
    ]
    assert sorted(pk) == ["id", "tenant_id"]


def test_merge_on_rerun_keeps_one_row_per_tenant_and_id(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        for tenant in (_INTERSTATE, _OSC):
            rsps.add(
                responses.GET,
                _url(tenant, "jpm", "export/jobs"),
                json=_export_body([{"id": 42, "jobStatus": "Scheduled"}]),
            )
        tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["jobs"]))
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        for tenant in (_INTERSTATE, _OSC):
            rsps.add(
                responses.GET,
                _url(tenant, "jpm", "export/jobs"),
                json=_export_body([{"id": 42, "jobStatus": "Completed"}]),
            )
        tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["jobs"]))

    rows = _query(tmp_pipeline, "SELECT tenant_id, id, job_status FROM jobs ORDER BY tenant_id")
    assert rows == [("111", 42, "Completed"), ("222", 42, "Completed")]


def test_incremental_second_run_sends_previous_high_water_mark(tmp_pipeline: Any) -> None:
    url = _url(_INTERSTATE, "jpm", "job-types")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json={
                "page": 1,
                "hasMore": False,
                "data": [
                    {"id": 1, "name": "Install", "modifiedOn": "2024-01-10T08:00:00Z"},
                    {"id": 2, "name": "Repair", "modifiedOn": "2024-03-05T10:00:00Z"},
                ],
            },
            match=[
                matchers.query_param_matcher(
                    {"active": "Any", "page": "1", "pageSize": "500"},
                )
            ],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["job_types"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json={
                "page": 1,
                "hasMore": False,
                "data": [{"id": 2, "name": "Repair (AC)", "modifiedOn": "2024-04-01T00:00:00Z"}],
            },
            match=[
                matchers.query_param_matcher(
                    {
                        "active": "Any",
                        "page": "1",
                        "pageSize": "500",
                        "modifiedOnOrAfter": "2024-03-05T10:00:00Z",
                    },
                )
            ],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["job_types"]))

    rows = _query(tmp_pipeline, "SELECT id, name FROM job_types ORDER BY id")
    assert rows == [(1, "Install"), (2, "Repair (AC)")]


def test_incremental_state_is_per_tenant(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        for tenant, modified in (
            (_INTERSTATE, "2024-05-01T00:00:00Z"),
            (_OSC, "2023-01-01T00:00:00Z"),
        ):
            rsps.add(
                responses.GET,
                _url(tenant, "inventory", "vendors"),
                json={"hasMore": False, "data": [{"id": 9, "modifiedOn": modified}]},
            )
        tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["vendors"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        for tenant in (_INTERSTATE, _OSC):
            rsps.add(
                responses.GET,
                _url(tenant, "inventory", "vendors"),
                json={"hasMore": False, "data": []},
            )
        tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["vendors"]))
        sent = {
            c.request.url.split("/tenant/")[1].split("/")[0]: c.request.params.get(
                "modifiedOnOrAfter"
            )
            for c in rsps.calls
            if c.request.url.startswith(_API)
        }
    assert sent == {"111": "2024-05-01T00:00:00Z", "222": "2023-01-01T00:00:00Z"}


def test_start_date_seeds_first_run(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _url(_INTERSTATE, "jpm", "export/jobs"),
            json=_export_body([]),
            match=[matchers.query_param_matcher({"from": "2024-01-01"})],
        )
        job_types = rsps.add(
            responses.GET,
            _url(_INTERSTATE, "jpm", "job-types"),
            json={"hasMore": False, "data": []},
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["jobs", "job_types"], start_date="2024-01-01"))
    # Reference collections ignore start_date: a floored first load would drop every
    # lookup row not modified since then, and incremental runs never backfill them.
    assert job_types.call_count == 1
    assert "modifiedOnOrAfter" not in job_types.calls[0].request.url


def test_forbidden_resource_for_one_tenant_is_skipped(
    tmp_pipeline: Any, caplog: pytest.LogCaptureFixture
) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _url(_INTERSTATE, "jpm", "export/jobs"),
            json=_export_body([{"id": 1}]),
        )
        rsps.add(
            responses.GET,
            _url(_OSC, "jpm", "export/jobs"),
            json={"title": "Forbidden"},
            status=403,
        )
        for tenant in (_INTERSTATE, _OSC):
            rsps.add(
                responses.GET,
                _url(tenant, "crm", "export/customers"),
                json=_export_body([{"id": 7, "name": f"Customer {tenant['name']}"}]),
            )
        with caplog.at_level(logging.WARNING, logger="paradox_dlt_sources.servicetitan"):
            tmp_pipeline.run(_source([_INTERSTATE, _OSC], ["jobs", "customers"]))

    assert _query(tmp_pipeline, "SELECT tenant_id FROM jobs") == [("111",)]
    assert _query(tmp_pipeline, "SELECT tenant_id FROM customers ORDER BY tenant_id") == [
        ("111",),
        ("222",),
    ]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("403" in m and "OSC" in m and "'jobs'" in m for m in warnings)


def test_non_403_error_still_fails(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _url(_INTERSTATE, "jpm", "export/jobs"),
            json={"title": "Not Found"},
            status=404,
        )
        with pytest.raises(Exception, match="404"):
            tmp_pipeline.run(_source([_INTERSTATE], ["jobs"]))


def test_export_continuation_token_pagination_and_resume(tmp_pipeline: Any) -> None:
    url = _url(_INTERSTATE, "jpm", "export/jobs")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json=load_fixture("servicetitan", "export_jobs_page_1"),
            match=[matchers.query_param_matcher({})],
        )
        rsps.add(
            responses.GET,
            url,
            json=load_fixture("servicetitan", "export_jobs_page_2"),
            match=[matchers.query_param_matcher({"from": "token-page-2"})],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["jobs"]))
        assert len(_api_calls(rsps, url)) == 2

    assert _query(tmp_pipeline, "SELECT id FROM jobs ORDER BY id") == [(1001,), (1002,)]

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json=_export_body([], token="token-after-run-2"),
            match=[matchers.query_param_matcher({"from": "token-after-run-1"})],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["jobs"]))
        assert len(_api_calls(rsps, url)) == 1


def test_page_has_more_pagination(tmp_pipeline: Any) -> None:
    url = _url(_INTERSTATE, "forms", "submissions")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json=load_fixture("servicetitan", "form_submissions_page_1"),
            match=[matchers.query_param_matcher({"active": "Any", "page": "1", "pageSize": "2"})],
        )
        rsps.add(
            responses.GET,
            url,
            json=load_fixture("servicetitan", "form_submissions_page_2"),
            match=[matchers.query_param_matcher({"active": "Any", "page": "2", "pageSize": "2"})],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["form_submissions"], page_size=2))
        assert len(_api_calls(rsps, url)) == 2

    rows = _query(tmp_pipeline, "SELECT id, form_name, units FROM form_submissions ORDER BY id")
    assert [r[0] for r in rows] == [501, 502, 503]
    assert "R-410A" in str(rows[0][2])
    assert "form_submissions__units" not in tmp_pipeline.default_schema.tables


def test_form_submissions_incremental_on_submitted_on(tmp_pipeline: Any) -> None:
    url = _url(_INTERSTATE, "forms", "submissions")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, url, json=load_fixture("servicetitan", "form_submissions_page_2"))
        tmp_pipeline.run(_source([_INTERSTATE], ["form_submissions"]))
        assert "submittedOnOrAfter" not in _api_calls(rsps, url)[0].request.params

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            url,
            json={"hasMore": False, "data": []},
            match=[
                matchers.query_param_matcher(
                    {"submittedOnOrAfter": "2024-03-02T09:00:00Z"}, strict_match=False
                )
            ],
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["form_submissions"]))
        [call] = _api_calls(rsps, url)
        assert "modifiedOnOrAfter" not in call.request.params


def test_nested_items_land_in_child_table(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _url(_INTERSTATE, "accounting", "export/invoices"),
            json=_export_body(
                [
                    {
                        "id": 300,
                        "total": "150.00",
                        "items": [
                            {"id": 1, "skuName": "Capacitor", "total": "100.00"},
                            {"id": 2, "skuName": "Labor", "total": "50.00"},
                        ],
                    }
                ]
            ),
        )
        tmp_pipeline.run(_source([_INTERSTATE], ["invoices"]))

    assert _query(tmp_pipeline, "SELECT sku_name FROM invoices__items ORDER BY sku_name") == [
        ("Capacitor",),
        ("Labor",),
    ]


def test_retry_after_on_429(tmp_pipeline: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", slept.append)
    url = _url(_INTERSTATE, "jpm", "export/jobs")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, url, status=429, headers={"Retry-After": "3"}, json={})
        rsps.add(responses.GET, url, json=_export_body([{"id": 1}]))
        tmp_pipeline.run(_source([_INTERSTATE], ["jobs"]))
        assert len(_api_calls(rsps, url)) == 2

    assert 3 in slept
    assert _query(tmp_pipeline, "SELECT id FROM jobs") == [(1,)]


def test_resource_names_are_per_tenant() -> None:
    source = _source([_INTERSTATE, _OSC], ["jobs", "vendors"])
    assert sorted(source.resources.keys()) == ["jobs_111", "jobs_222", "vendors_111", "vendors_222"]


def test_unknown_resource_rejected() -> None:
    with pytest.raises(Exception, match="unknown resource"):
        _source([_INTERSTATE], ["nope"])


def test_tenants_resolve_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SOURCES__SERVICETITAN__TENANTS",
        '[{"tenant_id": "111", "name": "Interstate", "client_id": "c", "client_secret": "s"}]',
    )
    monkeypatch.setenv("SOURCES__SERVICETITAN__APP_KEY", "app-key")
    source = servicetitan_source(resources=["jobs"])
    assert list(source.resources.keys()) == ["jobs_111"]
