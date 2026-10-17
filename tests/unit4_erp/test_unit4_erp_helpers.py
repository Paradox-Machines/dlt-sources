"""Unit tests for unit4_erp helpers: auth, filters, paging and row transforms.

HTTP is mocked with ``responses`` (never pytest-httpx, per CONTRIBUTING.md).
"""

from __future__ import annotations

import base64
import datetime as dt
from zoneinfo import ZoneInfo

import pytest
import responses
from requests import Request

from paradox_dlt_sources.unit4_erp.helpers import (
    ClientCredentialsAuth,
    KeysetPaginator,
    RequestThrottle,
    and_filters,
    columns,
    key_select,
    keyset_filter,
    literal,
    make_auth,
    paging_keys,
    parse_timestamp,
    primary_key,
    timestamp_literal,
    tombstones,
    transform_row,
)
from paradox_dlt_sources.unit4_erp.settings import OBJECTS_BY_NAME, Key
from tests._helpers.fixture_loader import load_fixture

_TOKEN_URL = "https://ids.unit4cloud.test/connect/token"
_GL = OBJECTS_BY_NAME["general_ledger_transactions"]
_UTC = ZoneInfo("UTC")


# --- filters ----------------------------------------------------------------------


def test_literal_quotes_text_and_escapes_single_quotes() -> None:
    assert literal("O'Brien", Key("supplierId")) == "'O''Brien'"


def test_literal_leaves_numbers_and_dates_bare() -> None:
    assert literal("42", Key("transactionNumber", "bigint")) == "42"
    assert literal("2026-01-01", Key("dateFrom", "date")) == "2026-01-01"


def test_literal_refuses_null_key() -> None:
    with pytest.raises(ValueError, match="keyset paging cannot continue"):
        literal(None, Key("supplierId"))


def test_keyset_filter_selects_strictly_after_last_key() -> None:
    keys = paging_keys(_GL)
    assert keyset_filter(keys, [1500048427, 2]) == (
        "(transactionNumber gt 1500048427) or "
        "(transactionNumber eq 1500048427 and additionalInformation/sequenceNumber gt 2)"
    )


def test_and_filters_parenthesises_and_skips_empty() -> None:
    assert and_filters(None, "a eq 1") == "a eq 1"
    assert and_filters("a eq 1", None, "b gt 2") == "(a eq 1) and (b gt 2)"
    assert and_filters(None, "") is None


def test_key_select_uses_parent_child_form_for_nested_keys() -> None:
    assert (
        key_select(_GL.keys) == "companyId,transactionNumber,additionalInformation(sequenceNumber)"
    )


def test_paging_keys_drop_company_unless_it_is_the_only_key() -> None:
    assert [k.path for k in paging_keys(_GL)] == [
        "transactionNumber",
        "additionalInformation/sequenceNumber",
    ]
    assert [k.path for k in paging_keys(OBJECTS_BY_NAME["companies"])] == ["companyId"]


def test_timestamp_literal_uses_z_for_utc_and_offset_otherwise() -> None:
    value = dt.datetime(2026, 7, 3, 14, 0, tzinfo=dt.UTC)
    assert timestamp_literal(value, _UTC) == "2026-07-03T14:00:00Z"
    assert timestamp_literal(value, ZoneInfo("America/New_York")) == "2026-07-03T10:00:00-04:00"


# --- paging -----------------------------------------------------------------------


def test_paginator_rejects_page_size_over_unit4_cap() -> None:
    with pytest.raises(ValueError, match="page_size"):
        KeysetPaginator(paging_keys(_GL), 50_001)


def test_paginator_seeds_order_limit_and_base_filter() -> None:
    paginator = KeysetPaginator(
        paging_keys(_GL), 2, "lastUpdated/updatedAt ge 2026-01-01T00:00:00Z"
    )
    request = Request("GET", "https://x.test", params={})
    paginator.init_request(request)
    assert request.params == {
        "limit": 2,
        "orderBy": "transactionNumber,additionalInformation/sequenceNumber",
        "filter": "lastUpdated/updatedAt ge 2026-01-01T00:00:00Z",
    }


# --- timestamps and rows ----------------------------------------------------------


def test_parse_timestamp_handles_dotnet_fractions_and_naive_values() -> None:
    assert parse_timestamp("2026-07-03T14:00:00.1234567Z", _UTC) == dt.datetime(
        2026, 7, 3, 14, 0, 0, 123456, tzinfo=dt.UTC
    )
    eastern = ZoneInfo("America/New_York")
    assert parse_timestamp("2026-07-03T10:00:00", eastern) == dt.datetime(
        2026, 7, 3, 14, 0, tzinfo=dt.UTC
    )
    assert parse_timestamp(None, _UTC) is None


def test_transform_row_lifts_nested_keys_and_adds_standard_columns() -> None:
    raw = load_fixture("unit4_erp", "gl_transactions_page_1")[0]
    now = dt.datetime(2026, 7, 5, tzinfo=dt.UTC)
    row = transform_row(raw, _GL, "RM", _UTC, now)
    assert row["additionalInformation__sequenceNumber"] == 1
    assert row["additionalInformation"] == raw["additionalInformation"]
    assert row["_source_account_id"] == "RM"
    assert row["_source_updated_at"] == dt.datetime(2026, 7, 3, 14, 0, 0, 123456, tzinfo=dt.UTC)
    assert row["_is_deleted"] is False
    assert row["_extracted_at"] == now


def test_primary_key_and_columns_cover_account_and_keys() -> None:
    assert primary_key(_GL) == [
        "_source_account_id",
        "transactionNumber",
        "additionalInformation__sequenceNumber",
    ]
    hints = columns(_GL)
    assert hints["additionalInformation__sequenceNumber"]["data_type"] == "bigint"
    assert hints["_is_deleted"] == {"data_type": "bool", "nullable": False}
    assert hints["_source_updated_at"]["data_type"] == "timestamp"


def test_tombstones_keep_stored_data_and_skip_live_keys() -> None:
    when = dt.datetime(2026, 7, 5, tzinfo=dt.UTC)
    stored = [
        {"_source_account_id": "RM", "supplier_id": "S1", "supplier_name": "Acme"},
        {"_source_account_id": "RM", "supplier_id": "S2", "supplier_name": "Globex"},
    ]
    out = tombstones(stored, ["_source_account_id", "supplier_id"], {("RM", "S1")}, when)
    assert out == [
        {
            "_source_account_id": "RM",
            "supplier_id": "S2",
            "supplier_name": "Globex",
            "_is_deleted": True,
            "_deleted_at": when,
            "_extracted_at": when,
        }
    ]


# --- auth -------------------------------------------------------------------------


def test_make_auth_rejects_unknown_type_and_missing_keys() -> None:
    with pytest.raises(ValueError, match="credentials.type"):
        make_auth({"type": "api_key"})
    with pytest.raises(ValueError, match="client_secret"):
        make_auth({"type": "oauth_client_credentials", "token_url": _TOKEN_URL, "client_id": "c"})


def test_basic_auth_sends_erp_user() -> None:
    auth = make_auth({"type": "basic", "username": "PARADOX", "password": "pw"})
    request = Request("GET", "https://x.test").prepare()
    auth(request)
    assert request.headers["Authorization"] == "Basic " + base64.b64encode(b"PARADOX:pw").decode()


@responses.activate
def test_client_credentials_caches_token_and_sends_scope() -> None:
    responses.add(responses.POST, _TOKEN_URL, json=load_fixture("unit4_erp", "token_response"))
    auth = ClientCredentialsAuth(_TOKEN_URL, "cid", "secret", "u4erp")
    for _ in range(3):
        request = Request("GET", "https://x.test").prepare()
        auth(request)
        assert request.headers["Authorization"] == "Bearer token-1"
    assert len(responses.calls) == 1
    body = responses.calls[0].request.body
    assert "grant_type=client_credentials" in body
    assert "scope=u4erp" in body
    assert "secret" not in repr(auth)


@responses.activate
def test_client_credentials_refreshes_before_expiry() -> None:
    responses.add(responses.POST, _TOKEN_URL, json={"access_token": "old", "expires_in": 30})
    responses.add(responses.POST, _TOKEN_URL, json={"access_token": "new", "expires_in": 3600})
    auth = ClientCredentialsAuth(_TOKEN_URL, "cid", "secret", "u4erp")
    assert auth.access_token() == "old"
    # 30 s left is inside the 60 s safety margin, so the next call refreshes.
    assert auth.access_token() == "new"


def test_throttle_spaces_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 100.0}
    slept: list[float] = []
    monkeypatch.setattr("time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("time.sleep", slept.append)
    throttle = RequestThrottle(120)  # one slot every 0.5 s
    for _ in range(3):
        throttle.wait()
    assert slept == [0.5, 1.0]


def test_throttle_zero_disables() -> None:
    RequestThrottle(0).wait()
