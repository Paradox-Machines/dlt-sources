"""Unit tests for Sage Intacct auth, pagination and schema helpers."""

from __future__ import annotations

import json

import pytest
import requests
import responses

from paradox_dlt_sources.sage_intacct.helpers import (
    SageIntacctAuth,
    SageIntacctError,
    build_query,
    build_username,
    column_name,
    columns_for,
    entity_scopes,
    flatten_record,
    query_pages,
    to_row,
)
from paradox_dlt_sources.sage_intacct.settings import (
    OBJECTS,
    OBJECTS_BY_RESOURCE,
    TOKEN_EXPIRY_MARGIN_SECONDS,
)
from tests._helpers.fixture_loader import load_fixture
from tests.sage_intacct.fake_api import BASE_URL, QUERY_URL, TOKEN_URL, FakeSageApi, gl_line

GL_LINES = OBJECTS_BY_RESOURCE["journal_entry_lines"]


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _auth(clock: _Clock | None = None, username: str = "ws_user@interstateac") -> SageIntacctAuth:
    return SageIntacctAuth(
        client_id="cid",
        client_secret="csecret",
        username=username,
        base_url=BASE_URL,
        clock=clock or _Clock(),
    )


def test_build_username_top_level_and_entity() -> None:
    assert build_username("ws_user", "interstateac") == "ws_user@interstateac"
    assert build_username("ws_user", "interstateac", "") == "ws_user@interstateac"
    assert (
        build_username("ws_user", "interstateac", "Interstate") == "ws_user@interstateac|Interstate"
    )


def test_entity_scopes() -> None:
    assert entity_scopes(None, "") == [""]
    assert entity_scopes([], "") == [""]
    assert entity_scopes(["A", " B ", "A", ""], "") == ["A", "B"]


@responses.activate
def test_token_fetch_sends_client_credentials_and_caches() -> None:
    responses.add(responses.POST, TOKEN_URL, json=load_fixture("sage_intacct", "token"))
    auth = _auth(username="ws_user@interstateac|Interstate")

    assert auth.get_token() == "fixture-access-token"
    assert auth.get_token() == "fixture-access-token"

    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body) == {
        "grant_type": "client_credentials",
        "client_id": "cid",
        "client_secret": "csecret",
        "username": "ws_user@interstateac|Interstate",
    }


@responses.activate
def test_token_refreshes_on_expiry() -> None:
    responses.add(responses.POST, TOKEN_URL, json={"access_token": "first", "expires_in": 3600})
    responses.add(responses.POST, TOKEN_URL, json={"access_token": "second", "expires_in": 3600})
    clock = _Clock()
    auth = _auth(clock)

    assert auth.get_token() == "first"
    clock.now += 3600 - TOKEN_EXPIRY_MARGIN_SECONDS - 1
    assert auth.get_token() == "first"
    clock.now += 1
    assert auth.get_token() == "second"
    assert len(responses.calls) == 2


@responses.activate
def test_token_error_surfaces_ia_error() -> None:
    responses.add(
        responses.POST,
        TOKEN_URL,
        status=401,
        json={"ia::error": {"code": "invalidClient", "message": "Bad client"}},
    )
    with pytest.raises(SageIntacctError, match="invalidClient: Bad client"):
        _auth().get_token()


def test_column_name() -> None:
    assert column_name("audit.modifiedDateTime") == "audit__modified_date_time"
    assert column_name("dimensions.department.id") == "dimensions__department__id"
    assert column_name("webURL") == "web_url"
    assert column_name("key") == "key"


def test_flatten_nested_and_dotted_shapes_match() -> None:
    nested, dotted = load_fixture("sage_intacct", "journal_entry_lines_page")["ia::result"]
    flat_nested = flatten_record(nested)
    flat_dotted = flatten_record(dotted)
    assert flat_nested.keys() == flat_dotted.keys()
    assert flat_nested["glAccount.id"] == "5000"
    assert flat_nested["dimensions.department.id"] == "HVAC-SVC"


def test_to_row_tags_entity() -> None:
    row = to_row({"key": "1", "audit": {"modifiedDateTime": "x"}}, "Interstate")
    assert row == {"key": "1", "audit__modified_date_time": "x", "entity_id": "Interstate"}


def test_build_query_requests_explicit_fields_and_filter() -> None:
    body = build_query(GL_LINES, start=1001, size=1000, modified_since="2024-01-01T00:00:00Z")
    assert body["object"] == "general-ledger/journal-entry-line"
    assert body["fields"] == list(GL_LINES.fields)
    assert body["start"] == 1001
    assert body["size"] == 1000
    assert body["orderBy"] == [{"key": "asc"}]
    assert body["filters"] == [{"$gte": {"audit.modifiedDateTime": "2024-01-01T00:00:00Z"}}]
    assert "filters" not in build_query(GL_LINES, 1, 1000)


@responses.activate
def test_query_pages_follows_next_across_pages_of_1000() -> None:
    api = FakeSageApi({("", GL_LINES.object): [gl_line(i) for i in range(1, 2501)]})
    api.register(responses.mock)
    session = requests.Session()

    pages = list(query_pages(session, _auth(), GL_LINES, page_size=1000))

    assert [len(p) for p in pages] == [1000, 1000, 500]
    assert [body["start"] for _, body in api.queries] == [1, 1001, 2001]
    assert {body["size"] for _, body in api.queries} == {1000}
    assert len(api.token_requests) == 1


@responses.activate
def test_query_pages_refetches_token_after_401() -> None:
    responses.add(responses.POST, TOKEN_URL, json={"access_token": "stale", "expires_in": 43200})
    responses.add(responses.POST, TOKEN_URL, json={"access_token": "fresh", "expires_in": 43200})
    responses.add(responses.POST, QUERY_URL, status=401, json={"ia::error": {"code": "expired"}})
    responses.add(
        responses.POST,
        QUERY_URL,
        json={"ia::result": [gl_line(1)], "ia::meta": {"next": None}},
    )

    pages = list(query_pages(requests.Session(), _auth(), GL_LINES))

    assert len(pages) == 1
    auth_headers = [c.request.headers.get("Authorization") for c in responses.calls]
    assert auth_headers[1] == "Bearer stale"
    assert auth_headers[3] == "Bearer fresh"


@responses.activate
def test_query_pages_raises_on_error() -> None:
    responses.add(responses.POST, TOKEN_URL, json={"access_token": "t", "expires_in": 43200})
    responses.add(
        responses.POST,
        QUERY_URL,
        status=403,
        json={"ia::error": {"code": "permissionDenied", "message": "No GL access"}},
    )
    with pytest.raises(SageIntacctError, match="permissionDenied"):
        list(query_pages(requests.Session(), _auth(), GL_LINES))


@pytest.mark.parametrize("size", [0, 1001])
def test_query_pages_rejects_bad_page_size(size: int) -> None:
    with pytest.raises(ValueError, match="page_size"):
        list(query_pages(requests.Session(), _auth(), GL_LINES, page_size=size))


def test_columns_for_declares_every_field_and_compound_key() -> None:
    for obj in OBJECTS:
        columns = columns_for(obj)
        assert len(columns) == len(obj.fields) + 1
        assert columns["entity_id"]["primary_key"] is True
        assert columns["key"]["primary_key"] is True
        assert columns["key"]["nullable"] is False
        assert columns["audit__modified_date_time"]["data_type"] == "timestamp"
    gl = columns_for(GL_LINES)
    assert gl["txn_amount"]["data_type"] == "decimal"
    assert gl["entry_date"]["data_type"] == "date"
