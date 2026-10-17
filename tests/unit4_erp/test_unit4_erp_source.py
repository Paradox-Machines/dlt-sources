"""Integration tests for unit4_erp_source against a DuckDB destination.

All Unit4 auth and Object API HTTP is mocked with ``responses`` (never
pytest-httpx, per CONTRIBUTING.md).
"""

from __future__ import annotations

import copy
import datetime as dt
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import responses
from dlt.pipeline.exceptions import PipelineStepFailed

from paradox_dlt_sources.unit4_erp import unit4_erp_source
from tests._helpers.fixture_loader import load_fixture

_BASE = "https://rmu-acc.unit4cloud.test/api"
_TOKEN_URL = "https://ids.unit4cloud.test/connect/token"
_CREDENTIALS = {
    "type": "oauth_client_credentials",
    "token_url": _TOKEN_URL,
    "client_id": "cid",
    "client_secret": "secret",
}
_GL_URL = f"{_BASE}/v2/objects/general-ledger-transactions"
_ATTRIBUTES_URL = f"{_BASE}/v1/objects/attributes"
_SUPPLIERS_URL = f"{_BASE}/v1/objects/suppliers"
_EMPLOYEES_URL = f"{_BASE}/v1/objects/employees"


def _source(resources: list[str], **kwargs: Any) -> Any:
    options: dict[str, Any] = {
        "credentials": _CREDENTIALS,
        "base_url": _BASE,
        "company_ids": ["RM"],
        "resources": resources,
        "max_requests_per_minute": 0,
        **kwargs,
    }
    return unit4_erp_source(**options)


def _register_token(rsps: responses.RequestsMock) -> None:
    rsps.add(responses.POST, _TOKEN_URL, json=load_fixture("unit4_erp", "token_response"))


def _query(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


def _params(call: Any) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(call.request.url).query).items()}


def _calls(rsps: responses.RequestsMock, url: str) -> list[Any]:
    return [c for c in rsps.calls if c.request.url.split("?")[0] == url]


def _supplier(supplier_id: str, name: str, updated: str = "2026-07-01T00:00:00Z") -> dict:
    return {
        "companyId": "RM",
        "supplierId": supplier_id,
        "supplierName": name,
        "lastUpdated": {"updatedAt": updated, "updatedBy": "SYSTEM"},
    }


def _attribute(attribute_id: str, name: str) -> dict:
    return {"companyId": "RM", "attributeId": attribute_id, "attributeName": name, "dataLength": 25}


# --- paging -----------------------------------------------------------------------


def test_keyset_paging_walks_pages_and_loads_nested_keys(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _GL_URL, json=load_fixture("unit4_erp", "gl_transactions_page_1"))
        rsps.add(responses.GET, _GL_URL, json=load_fixture("unit4_erp", "gl_transactions_page_2"))
        tmp_pipeline.run(_source(["general_ledger_transactions"], page_size=2))
        first, second = (_params(c) for c in _calls(rsps, _GL_URL))

    assert first["companyId"] == "RM"
    assert first["select"] == "*.*"
    assert first["limit"] == "2"
    assert first["orderBy"] == "transactionNumber,additionalInformation/sequenceNumber"
    assert "filter" not in first
    assert second["filter"] == (
        "(transactionNumber gt 1500048427) or "
        "(transactionNumber eq 1500048427 and additionalInformation/sequenceNumber gt 2)"
    )
    rows = _query(
        tmp_pipeline,
        "SELECT _source_account_id, transaction_number, additional_information__sequence_number, "
        "_is_deleted FROM general_ledger_transactions ORDER BY 2, 3",
    )
    assert rows == [
        ("RM", 1500048427, 1, False),
        ("RM", 1500048427, 2, False),
        ("RM", 1500048428, 1, False),
    ]


def test_full_last_page_is_followed_by_an_empty_page(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _GL_URL, json=load_fixture("unit4_erp", "gl_transactions_page_1"))
        rsps.add(responses.GET, _GL_URL, json=[])
        tmp_pipeline.run(_source(["general_ledger_transactions"], page_size=2))
        assert len(_calls(rsps, _GL_URL)) == 2

    assert _query(tmp_pipeline, "SELECT count(*) FROM general_ledger_transactions") == [(2,)]


def test_page_larger_than_limit_fails_the_resource(tmp_pipeline: Any) -> None:
    rows = load_fixture("unit4_erp", "gl_transactions_page_1")
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _GL_URL, json=rows)
        with pytest.raises(PipelineStepFailed, match="ignored the page size"):
            tmp_pipeline.run(_source(["general_ledger_transactions"], page_size=1))


def test_429_is_retried_after_retry_after(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, status=429, headers={"Retry-After": "0"})
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[_supplier("S1", "Acme")])
        tmp_pipeline.run(_source(["suppliers"]))

    assert _query(tmp_pipeline, "SELECT supplier_id FROM suppliers") == [("S1",)]


# --- change capture ---------------------------------------------------------------


def test_second_run_filters_on_cursor_minus_lookback(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _SUPPLIERS_URL,
            json=[_supplier("S1", "Acme"), _supplier("S2", "Globex", "2026-07-02T08:00:00Z")],
        )
        tmp_pipeline.run(_source(["suppliers"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET,
            _SUPPLIERS_URL,
            json=[_supplier("S2", "Globex Corp", "2026-07-03T08:00:00Z")],
        )
        tmp_pipeline.run(_source(["suppliers"], delete_reconciliation_interval=None))
        (call,) = _calls(rsps, _SUPPLIERS_URL)

    assert _params(call)["filter"] == "lastUpdated/updatedAt ge 2026-07-02T07:00:00Z"
    assert _query(tmp_pipeline, "SELECT supplier_id, supplier_name FROM suppliers ORDER BY 1") == [
        ("S1", "Acme"),
        ("S2", "Globex Corp"),
    ]


def test_start_date_seeds_the_first_cursor(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[])
        tmp_pipeline.run(
            _source(
                ["suppliers"],
                start_date="2026-01-01",
                lookback=dt.timedelta(0),
                delete_reconciliation_interval=None,
            )
        )
        (call,) = _calls(rsps, _SUPPLIERS_URL)

    assert _params(call)["filter"] == "lastUpdated/updatedAt ge 2026-01-01T00:00:00Z"


def test_cursor_does_not_advance_when_the_run_fails(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[_supplier("S1", "Acme")])
        tmp_pipeline.run(_source(["suppliers"]))

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, status=400, json={"message": "bad filter"})
        with pytest.raises(PipelineStepFailed):
            tmp_pipeline.run(_source(["suppliers"], max_attempts=1))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[])
        tmp_pipeline.run(_source(["suppliers"], delete_reconciliation_interval=None))
        (call,) = _calls(rsps, _SUPPLIERS_URL)

    assert _params(call)["filter"] == "lastUpdated/updatedAt ge 2026-06-30T23:00:00Z"


# --- deletes ----------------------------------------------------------------------


def test_full_read_object_tombstones_missing_rows_and_restores_them(tmp_pipeline: Any) -> None:
    both = [_attribute("C1", "Cost center"), _attribute("F1", "Fund")]
    for body in (both, both[:1], both):
        with responses.RequestsMock() as rsps:
            _register_token(rsps)
            rsps.add(responses.GET, _ATTRIBUTES_URL, json=copy.deepcopy(body))
            tmp_pipeline.run(_source(["attributes"]))
        if body is not both:
            deleted = _query(
                tmp_pipeline,
                "SELECT attribute_id, attribute_name, data_length, _is_deleted, "
                "_deleted_at IS NOT NULL FROM attributes ORDER BY 1",
            )
            # The tombstone keeps the stored data (CONNECTOR_SPEC §12.1).
            assert deleted == [
                ("C1", "Cost center", 25, False, False),
                ("F1", "Fund", 25, True, True),
            ]

    assert _query(tmp_pipeline, "SELECT attribute_id, _is_deleted FROM attributes ORDER BY 1") == [
        ("C1", False),
        ("F1", False),
    ]


def test_incremental_object_reconciles_keys_on_interval(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(
            responses.GET, _SUPPLIERS_URL, json=[_supplier("S1", "Acme"), _supplier("S2", "Globex")]
        )
        tmp_pipeline.run(_source(["suppliers"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[])  # incremental: no changes
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[{"companyId": "RM", "supplierId": "S1"}])
        tmp_pipeline.run(_source(["suppliers"], delete_reconciliation_interval=dt.timedelta(0)))
        incremental, key_read = (_params(c) for c in _calls(rsps, _SUPPLIERS_URL))

    assert "filter" in incremental
    assert key_read["select"] == "companyId,supplierId"
    assert "filter" not in key_read
    assert _query(
        tmp_pipeline, "SELECT supplier_id, supplier_name, _is_deleted FROM suppliers ORDER BY 1"
    ) == [("S1", "Acme", False), ("S2", "Globex", True)]


def test_reconciliation_is_skipped_until_the_interval_elapses(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[_supplier("S1", "Acme")])
        tmp_pipeline.run(_source(["suppliers"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _SUPPLIERS_URL, json=[])
        tmp_pipeline.run(_source(["suppliers"]))  # default interval: one day
        assert len(_calls(rsps, _SUPPLIERS_URL)) == 1


def test_empty_key_read_does_not_tombstone_everything(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _ATTRIBUTES_URL, json=[_attribute("C1", "Cost center")])
        tmp_pipeline.run(_source(["attributes"]))

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _ATTRIBUTES_URL, json=[])
        with pytest.raises(PipelineStepFailed, match="refusing to mark them all deleted"):
            tmp_pipeline.run(_source(["attributes"]))


# --- companies, resources, metadata -----------------------------------------------


def test_two_companies_with_the_same_key_do_not_collide(tmp_pipeline: Any) -> None:
    def by_company(request: Any) -> tuple[int, dict[str, str], str]:
        company = parse_qs(urlparse(request.url).query)["companyId"][0]
        row = {**_supplier("S1", f"Acme {company}"), "companyId": company}
        return 200, {}, json.dumps([row])

    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add_callback(responses.GET, _SUPPLIERS_URL, callback=by_company)
        tmp_pipeline.run(_source(["suppliers"], company_ids=["RM", "RF"]))

    assert _query(
        tmp_pipeline, "SELECT _source_account_id, supplier_name FROM suppliers ORDER BY 1"
    ) == [("RF", "Acme RF"), ("RM", "Acme RM")]


def test_default_resources_skip_restricted_payroll() -> None:
    names = {
        r.name
        for r in unit4_erp_source(
            credentials=_CREDENTIALS, base_url=_BASE, company_ids=["RM"]
        ).resources.values()
    }
    assert "general_ledger_transactions_rm" in names
    assert "field_definitions_rm" in names
    assert "payroll_transactions_rm" not in names
    assert "employee_rates_rm" not in names


def test_restricted_object_loads_when_named() -> None:
    source = _source(["payroll_transactions"])
    assert list(source.resources) == ["payroll_transactions_rm"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"resources": ["nope"]}, "unknown resource"),
        ({"company_ids": []}, "at least one company"),
        ({"company_ids": ["RM", "RM"]}, "duplicates"),
        ({"credentials": {"type": "basic", "username": "u"}}, "password"),
    ],
)
def test_invalid_config_raises(kwargs: dict[str, Any], message: str) -> None:
    options = {"resources": ["suppliers"], **kwargs}
    with pytest.raises(Exception, match=message):
        _source(**options)


def test_field_definitions_load_runtime_schema_with_custom_fields(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register_token(rsps)
        rsps.add(responses.GET, _EMPLOYEES_URL, json=load_fixture("unit4_erp", "employees_schema"))
        tmp_pipeline.run(
            _source(["employees", "field_definitions"]).with_resources("field_definitions_rm")
        )
        (call,) = _calls(rsps, _EMPLOYEES_URL)

    assert call.request.headers["Accept"] == "application/schema+json"
    rows = _query(
        tmp_pipeline,
        "SELECT field_key, label, data_type, is_custom, is_mandatory FROM field_definitions "
        "WHERE object_type = 'employees' ORDER BY 1",
    )
    assert ("birthDate", "Birth date", "string:date-time", False, False) in rows
    assert ("companyId", "Company", "string", False, True) in rows
    assert (
        "customFieldGroups/ipeds/raceEthnicity",
        "IPEDS race/ethnicity",
        "string",
        True,
        False,
    ) in rows
    assert any(r[0] == "lastUpdated/updatedAt" for r in rows)
