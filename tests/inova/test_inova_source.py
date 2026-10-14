"""Integration tests for the inova_source factory.

Mocks UKG Ready endpoints with ``responses`` and runs the source into duckdb.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest
import responses
from responses import matchers

from paradox_dlt_sources.inova import inova_source
from tests._helpers.fixture_loader import load_fixture, register_get

_CID = "123456"
_BASE = f"https://pod.example.com/ta/rest/v2/companies/{_CID}"


def _source(**kwargs: Any) -> Any:
    return inova_source(
        company_id=_CID,
        pod="unused",
        client_id="cid",
        client_secret="secret",
        base_url=_BASE,
        **kwargs,
    )


def _register_common(rsps: responses.RequestsMock) -> None:
    rsps.add(
        responses.POST,
        f"{_BASE}/oauth2/token",
        json=load_fixture("inova", "token"),
        match=[
            matchers.urlencoded_params_matcher(
                {"grant_type": "client_credentials", "client_id": "cid", "client_secret": "secret"}
            )
        ],
    )
    register_get(rsps, f"{_BASE}/lookup/eins", load_fixture("inova", "eins"))
    register_get(rsps, f"{_BASE}/config/cost-centers", load_fixture("inova", "cost_centers_tree_0"))
    register_get(rsps, f"{_BASE}/employees", load_fixture("inova", "employees"))
    register_get(
        rsps, f"{_BASE}/compensation/history", load_fixture("inova", "compensation_history")
    )
    register_get(rsps, f"{_BASE}/payrolls", load_fixture("inova", "payrolls"))
    register_get(
        rsps,
        f"{_BASE}/payrolls/312320/pay-statements",
        load_fixture("inova", "pay_statements_312320"),
    )
    register_get(
        rsps,
        f"{_BASE}/payrolls/312320/pay-statements/280064",
        load_fixture("inova", "pay_statement_280064"),
    )


def _rows(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


@responses.activate
def test_inova_source_loads_all_tables(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source(payroll_start_date="2025-01-01"))

    counts = {
        t: _rows(tmp_pipeline, f"select count(*) from {t}")[0][0]
        for t in (
            "companies",
            "cost_centers",
            "employees",
            "compensation_history",
            "payrolls",
            "pay_statements",
            "pay_statement_earnings",
            "pay_statement_deductions",
            "pay_statement_taxes",
        )
    }
    assert counts == {
        "companies": 2,
        "cost_centers": 3,
        "employees": 2,
        "compensation_history": 3,
        "payrolls": 1,
        "pay_statements": 1,
        "pay_statement_earnings": 2,
        "pay_statement_deductions": 1,
        "pay_statement_taxes": 1,
    }


@responses.activate
def test_every_row_carries_company_and_legal_entity(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source(payroll_start_date="2025-01-01"))

    for table in (
        "companies",
        "cost_centers",
        "employees",
        "compensation_history",
        "payrolls",
        "pay_statements",
        "pay_statement_earnings",
        "pay_statement_deductions",
        "pay_statement_taxes",
    ):
        values = {r[0] for r in _rows(tmp_pipeline, f"select distinct company_id from {table}")}
        assert values == {_CID}, table

    assert dict(_rows(tmp_pipeline, "select id, ein_id from employees")) == {3588: 101, 3589: 102}
    assert dict(_rows(tmp_pipeline, "select id, ein_id from compensation_history")) == {
        9001: 101,
        9002: 101,
        9003: 102,
    }
    assert {r[0] for r in _rows(tmp_pipeline, "select ein_id from pay_statement_earnings")} == {101}


@responses.activate
def test_pii_fields_are_excluded(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source(payroll_start_date="2025-01-01"))

    columns = {
        r[0]
        for r in _rows(
            tmp_pipeline,
            "select column_name from information_schema.columns where table_name = 'employees'",
        )
    }
    assert "social_security" not in columns
    assert "birthday" not in columns
    assert "first_name" in columns


@responses.activate
def test_pay_statement_lines_link_to_statement(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source(payroll_start_date="2025-01-01"))

    rows = _rows(
        tmp_pipeline,
        "select id, payroll_id, pay_statement_id, account_id, ee_amount "
        "from pay_statement_earnings order by id",
    )
    assert rows == [
        (361984, 312320, 280064, 3588, 2423.2),
        (361985, 312320, 280064, 3588, 181.76),
    ]
    taxes = _rows(tmp_pipeline, "select er_amount, company_tax__code from pay_statement_taxes")
    assert taxes == [(161.6, "FICA")]


@responses.activate
def test_payrolls_request_bounded_pay_date_windows(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source(payroll_start_date="2024-01-01").with_resources("payrolls"))

    calls = [c.request for c in responses.calls if "/payrolls" in (c.request.url or "")]
    assert len(calls) >= 2
    first = calls[0].params
    assert first["pay_date_from"] == "2024-01-01"
    for call in calls:
        lo = datetime.fromisoformat(call.params["pay_date_from"])
        hi = datetime.fromisoformat(call.params["pay_date_to"])
        assert (hi - lo).days < 365


@responses.activate
def test_employees_second_run_uses_changed_since_start_value(tmp_pipeline):
    _register_common(responses.mock)
    tmp_pipeline.run(_source().with_resources("employees"))

    first_stamp = _rows(tmp_pipeline, "select max(_synced_at) from employees")[0][0]
    register_get(
        responses.mock, f"{_BASE}/employees/changed", load_fixture("inova", "employees_changed")
    )
    tmp_pipeline.run(_source().with_resources("employees"))

    changed = [c.request for c in responses.calls if "/employees/changed" in (c.request.url or "")]
    assert len(changed) == 1
    since = datetime.fromisoformat(changed[0].params["since"])
    assert timedelta(0) <= first_stamp - since < timedelta(seconds=1)

    rows = dict(_rows(tmp_pipeline, "select id, _deleted from employees"))
    assert rows == {3588: False, 3589: True, 3590: False}
    columns = {
        r[0]
        for r in _rows(
            tmp_pipeline,
            "select column_name from information_schema.columns where table_name = 'employees'",
        )
    }
    assert "social_security" not in columns


def test_invalid_cost_center_tree_index_rejected():
    with pytest.raises(ValueError, match="0..8"):
        _source(cost_center_tree_indexes=(9,))
