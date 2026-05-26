"""Unit tests for QuickBooks source helpers.

Covers:
- RotatingRefreshTokenAuth: token exchange, rotation detection, callback firing
- QuickBooksQueryPaginator: state update, request mutation
- build_query: SQL string construction
- coerce_metadata_last_updated_time: row transform
- columns / nullable_column: schema hint builders
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

import pendulum
import pytest
import responses
from requests import PreparedRequest, Request, Response
from requests.exceptions import HTTPError

from paradox_dlt_sources.quickbooks.helpers import (
    QuickBooksQueryPaginator,
    RotatingRefreshTokenAuth,
    build_query,
    coerce_metadata_last_updated_time,
    columns,
    nullable_column,
)
from paradox_dlt_sources.quickbooks.settings import MAX_RESULTS

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
CLIENT_ID = "test-client-id"
CLIENT_SECRET = "test-client-secret"
REFRESH_TOKEN = "refresh-token-original"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _response_with_body(body: dict[str, Any]) -> Response:
    r = Response()
    r._content = json.dumps(body).encode()
    r.status_code = 200
    return r


def _make_auth(
    on_token_rotation: Any = None,
    refresh_token: str = REFRESH_TOKEN,
) -> RotatingRefreshTokenAuth:
    if on_token_rotation is None:
        on_token_rotation = lambda _: None  # noqa: E731
    return RotatingRefreshTokenAuth(
        token_url=TOKEN_URL,
        refresh_token=refresh_token,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        on_token_rotation=on_token_rotation,
    )


# ---------------------------------------------------------------------------
# RotatingRefreshTokenAuth — basic token exchange
# ---------------------------------------------------------------------------


@responses.activate
def test_auth_fetches_access_token_on_first_request() -> None:
    """First request triggers a token exchange and sets Authorization header."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "access-v1",
            "expires_in": 3600,
            "refresh_token": REFRESH_TOKEN,
        },
        status=200,
    )

    auth = _make_auth()
    req = PreparedRequest()
    req.prepare_headers({})
    result = auth(req)

    assert result.headers["Authorization"] == "Bearer access-v1"
    assert len(responses.calls) == 1


@responses.activate
def test_auth_uses_http_basic_not_form_body() -> None:
    """Client credentials must go in Authorization: Basic, NOT the form body."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok", "expires_in": 3600, "refresh_token": REFRESH_TOKEN},
        status=200,
    )

    auth = _make_auth()
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    token_call = responses.calls[0]
    # Verify Basic auth header
    auth_header = token_call.request.headers.get("Authorization", "")
    assert auth_header.startswith("Basic ")
    decoded = base64.b64decode(auth_header[len("Basic ") :]).decode()
    assert decoded == f"{CLIENT_ID}:{CLIENT_SECRET}"
    # Verify credentials are NOT in the request body
    body = token_call.request.body or ""
    assert "client_id" not in body
    assert "client_secret" not in body


@responses.activate
def test_auth_passes_refresh_token_in_form_body() -> None:
    """refresh_token must appear in the POST body alongside grant_type."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok", "expires_in": 3600, "refresh_token": REFRESH_TOKEN},
        status=200,
    )

    auth = _make_auth()
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    body = responses.calls[0].request.body or ""
    assert "grant_type=refresh_token" in body
    assert f"refresh_token={REFRESH_TOKEN}" in body


@responses.activate
def test_auth_caches_access_token_for_subsequent_requests() -> None:
    """Second request within expiry window reuses cached token (no second call)."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "cached-tok", "expires_in": 3600, "refresh_token": REFRESH_TOKEN},
        status=200,
    )

    auth = _make_auth()
    req1 = PreparedRequest()
    req1.prepare_headers({})
    req2 = PreparedRequest()
    req2.prepare_headers({})

    auth(req1)
    auth(req2)

    assert len(responses.calls) == 1  # only one token exchange
    assert req2.headers["Authorization"] == "Bearer cached-tok"


@responses.activate
def test_auth_refreshes_when_token_expired() -> None:
    """Token is re-fetched when the cached one has expired."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok-1", "expires_in": 3600, "refresh_token": REFRESH_TOKEN},
        status=200,
    )
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok-2", "expires_in": 3600, "refresh_token": REFRESH_TOKEN},
        status=200,
    )

    auth = _make_auth()
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)
    assert req.headers["Authorization"] == "Bearer tok-1"

    # Force expiry
    auth._expires_at = time.time() - 1

    req2 = PreparedRequest()
    req2.prepare_headers({})
    auth(req2)
    assert req2.headers["Authorization"] == "Bearer tok-2"
    assert len(responses.calls) == 2


# ---------------------------------------------------------------------------
# RotatingRefreshTokenAuth — rotation detection + callback
# ---------------------------------------------------------------------------


@responses.activate
def test_rotation_callback_fires_when_new_refresh_token_returned() -> None:
    """on_token_rotation is called with the new token when Intuit rotates it."""
    rotated: list[str] = []

    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "access-v2",
            "expires_in": 3600,
            "refresh_token": "refresh-token-rotated",  # different from original
        },
        status=200,
    )

    auth = _make_auth(on_token_rotation=rotated.append)
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    assert rotated == ["refresh-token-rotated"]


@responses.activate
def test_rotation_callback_receives_correct_new_token() -> None:
    """The value passed to on_token_rotation is exactly the new token from the response."""
    received: list[str] = []

    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "access-new",
            "expires_in": 3600,
            "refresh_token": "brand-new-refresh-XYZ",
        },
        status=200,
    )

    auth = _make_auth(on_token_rotation=received.append, refresh_token="old-token")
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    assert received == ["brand-new-refresh-XYZ"]


@responses.activate
def test_rotation_callback_not_fired_when_token_unchanged() -> None:
    """on_token_rotation is NOT called when Intuit echoes back the same refresh token."""
    rotated: list[str] = []

    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "access-v1",
            "expires_in": 3600,
            "refresh_token": REFRESH_TOKEN,  # same as initial
        },
        status=200,
    )

    auth = _make_auth(on_token_rotation=rotated.append)
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    assert rotated == []


@responses.activate
def test_rotation_callback_not_fired_when_refresh_token_absent() -> None:
    """on_token_rotation is NOT called when the response omits refresh_token entirely."""
    rotated: list[str] = []

    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "access-v1",
            "expires_in": 3600,
            # no refresh_token key at all
        },
        status=200,
    )

    auth = _make_auth(on_token_rotation=rotated.append)
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)

    assert rotated == []


@responses.activate
def test_rotated_token_used_in_next_refresh() -> None:
    """After rotation, the new refresh token is used for the next exchange."""
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "tok-1",
            "expires_in": 3600,
            "refresh_token": "new-refresh-after-rotation",
        },
        status=200,
    )
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "tok-2",
            "expires_in": 3600,
            "refresh_token": "new-refresh-after-rotation",
        },
        status=200,
    )

    auth = _make_auth(refresh_token="old-refresh")
    req = PreparedRequest()
    req.prepare_headers({})
    auth(req)
    # Force expiry so we get a second exchange
    auth._expires_at = time.time() - 1

    req2 = PreparedRequest()
    req2.prepare_headers({})
    auth(req2)

    # Second call must have sent the rotated token, not the original
    second_body = responses.calls[1].request.body or ""
    assert "new-refresh-after-rotation" in second_body
    assert "old-refresh" not in second_body


@responses.activate
def test_auth_raises_on_http_error() -> None:
    """Token endpoint 4xx surfaces as requests.HTTPError."""
    responses.add(responses.POST, TOKEN_URL, status=401)

    auth = _make_auth()
    req = PreparedRequest()
    req.prepare_headers({})

    with pytest.raises(HTTPError):
        auth(req)


@responses.activate
def test_rotation_callback_called_once_per_rotation_event() -> None:
    """Multiple token exchanges without rotation do not accumulate callback calls."""
    rotated: list[str] = []

    # First exchange: rotation
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok-1", "expires_in": 3600, "refresh_token": "new-refresh"},
        status=200,
    )
    # Second exchange: no rotation (same token echoed back)
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok-2", "expires_in": 3600, "refresh_token": "new-refresh"},
        status=200,
    )

    auth = _make_auth(on_token_rotation=rotated.append)
    req1 = PreparedRequest()
    req1.prepare_headers({})
    auth(req1)
    # Only one callback so far
    assert rotated == ["new-refresh"]

    auth._expires_at = time.time() - 1
    req2 = PreparedRequest()
    req2.prepare_headers({})
    auth(req2)
    # Still only one callback — second exchange echoed same token
    assert rotated == ["new-refresh"]


# ---------------------------------------------------------------------------
# QuickBooksQueryPaginator
# ---------------------------------------------------------------------------


_BASE_QUERY = (
    "SELECT * FROM Invoice ORDER BY MetaData.LastUpdatedTime STARTPOSITION 1 MAXRESULTS 1000"
)
_SIMPLE_QUERY = "SELECT * FROM Invoice STARTPOSITION 1 MAXRESULTS 1000"


def test_paginator_advances_when_full_page() -> None:
    """A full page (MAX_RESULTS rows) marks has_next_page=True and advances position."""
    p = QuickBooksQueryPaginator(entity="Invoice", base_query=_BASE_QUERY)
    rows = [{"Id": str(i)} for i in range(MAX_RESULTS)]
    body = {"QueryResponse": {"Invoice": rows}}
    p.update_state(_response_with_body(body))

    assert p._has_next_page is True
    assert p._start_position == MAX_RESULTS + 1


def test_paginator_stops_on_short_page() -> None:
    """A short page (< MAX_RESULTS rows) marks has_next_page=False."""
    p = QuickBooksQueryPaginator(entity="Invoice", base_query=_BASE_QUERY)
    rows = [{"Id": "1"}, {"Id": "2"}]
    body = {"QueryResponse": {"Invoice": rows}}
    p.update_state(_response_with_body(body))

    assert p._has_next_page is False


def test_paginator_stops_on_empty_page() -> None:
    """An empty page marks has_next_page=False."""
    p = QuickBooksQueryPaginator(entity="Invoice", base_query=_SIMPLE_QUERY)
    body: dict[str, Any] = {"QueryResponse": {}}
    p.update_state(_response_with_body(body))

    assert p._has_next_page is False


def test_paginator_updates_query_param_in_request() -> None:
    """update_request rewrites the ``query`` param dict with the advanced STARTPOSITION.

    dlt passes a ``requests.Request`` (not PreparedRequest) to ``update_request``;
    params are stored in ``request.params`` dict and encoded at prepare-time.
    """
    p = QuickBooksQueryPaginator(entity="Invoice", base_query=_BASE_QUERY)
    # Simulate having seen a full first page
    p._start_position = 1001

    req = Request(
        method="GET",
        url="https://quickbooks.api.intuit.com/v3/company/123/query",
        params={"query": _BASE_QUERY, "minorversion": "65"},
    )
    p.update_request(req)

    assert isinstance(req.params, dict)
    new_query = req.params["query"]
    assert "STARTPOSITION 1001" in new_query
    assert "MAXRESULTS 1000" in new_query


def test_paginator_two_full_pages_advances_correctly() -> None:
    """Two consecutive full pages advance position by MAX_RESULTS each time."""
    p = QuickBooksQueryPaginator(entity="Invoice", base_query=_SIMPLE_QUERY)
    full_rows = [{"Id": str(i)} for i in range(MAX_RESULTS)]

    p.update_state(_response_with_body({"QueryResponse": {"Invoice": full_rows}}))
    assert p._start_position == MAX_RESULTS + 1

    p.update_state(_response_with_body({"QueryResponse": {"Invoice": full_rows}}))
    assert p._start_position == (MAX_RESULTS * 2) + 1


# ---------------------------------------------------------------------------
# build_query
# ---------------------------------------------------------------------------


def test_build_query_no_cursor() -> None:
    """Without a start value, no WHERE clause is added."""
    q = build_query(entity="Invoice", start_value_iso=None, start_position=1)
    assert "WHERE" not in q
    assert "SELECT * FROM Invoice" in q
    assert "STARTPOSITION 1" in q
    assert f"MAXRESULTS {MAX_RESULTS}" in q


def test_build_query_with_cursor() -> None:
    """With a start value, WHERE clause references MetaData.LastUpdatedTime."""
    iso = "2026-01-01T00:00:00+00:00"
    q = build_query(entity="Customer", start_value_iso=iso, start_position=1)
    assert f"WHERE MetaData.LastUpdatedTime > '{iso}'" in q
    assert "SELECT * FROM Customer" in q


def test_build_query_startposition_propagated() -> None:
    """start_position is correctly embedded in the query."""
    q = build_query(entity="Bill", start_value_iso=None, start_position=501)
    assert "STARTPOSITION 501" in q


def test_build_query_order_by_present() -> None:
    """ORDER BY MetaData.LastUpdatedTime is always present."""
    q = build_query(entity="Payment", start_value_iso=None, start_position=1)
    assert "ORDER BY MetaData.LastUpdatedTime" in q


# ---------------------------------------------------------------------------
# coerce_metadata_last_updated_time
# ---------------------------------------------------------------------------


def test_coerce_converts_iso_string_to_datetime() -> None:
    """ISO string in MetaData.LastUpdatedTime is parsed to pendulum.DateTime."""
    row: dict[str, Any] = {
        "Id": "1",
        "MetaData": {"LastUpdatedTime": "2026-03-15T10:30:00-07:00"},
    }
    out = coerce_metadata_last_updated_time(row)
    updated = out["MetaData"]["LastUpdatedTime"]
    assert isinstance(updated, pendulum.DateTime)


def test_coerce_preserves_other_metadata_fields() -> None:
    """Other MetaData fields are not dropped by the transform."""
    row: dict[str, Any] = {
        "Id": "1",
        "MetaData": {
            "CreateTime": "2026-01-01T00:00:00-08:00",
            "LastUpdatedTime": "2026-03-15T10:30:00-07:00",
        },
    }
    out = coerce_metadata_last_updated_time(row)
    assert "CreateTime" in out["MetaData"]


def test_coerce_no_op_when_no_metadata() -> None:
    """Row without MetaData key is returned unchanged."""
    row: dict[str, Any] = {"Id": "1", "DocNumber": "INV-001"}
    out = coerce_metadata_last_updated_time(row)
    assert out == row


def test_coerce_no_op_when_already_not_string() -> None:
    """Row where LastUpdatedTime is already non-string is returned unchanged."""
    dt = pendulum.now()
    row: dict[str, Any] = {"Id": "1", "MetaData": {"LastUpdatedTime": dt}}
    out = coerce_metadata_last_updated_time(row)
    assert out["MetaData"]["LastUpdatedTime"] is dt


def test_coerce_returns_new_dict_not_mutated() -> None:
    """The transform returns a new dict; the input row is not mutated."""
    row: dict[str, Any] = {
        "Id": "1",
        "MetaData": {"LastUpdatedTime": "2026-01-01T00:00:00+00:00"},
    }
    out = coerce_metadata_last_updated_time(row)
    assert out is not row
    # Original still has a string
    assert isinstance(row["MetaData"]["LastUpdatedTime"], str)


# ---------------------------------------------------------------------------
# columns / nullable_column
# ---------------------------------------------------------------------------


def test_columns_builds_text_hints() -> None:
    out = columns(text=("doc_number", "status"))
    assert out == {
        "doc_number": {"data_type": "text", "nullable": True},
        "status": {"data_type": "text", "nullable": True},
    }


def test_columns_builds_bigint_hints() -> None:
    out = columns(bigint=("amount_cents",))
    assert out == {"amount_cents": {"data_type": "bigint", "nullable": True}}


def test_columns_builds_decimal_hints() -> None:
    out = columns(decimal=("qty_on_hand", "unit_price"))
    assert out == {
        "qty_on_hand": {"data_type": "decimal", "nullable": True},
        "unit_price": {"data_type": "decimal", "nullable": True},
    }


def test_columns_builds_boolean_hints() -> None:
    out = columns(boolean=("active", "taxable"))
    assert out == {
        "active": {"data_type": "bool", "nullable": True},
        "taxable": {"data_type": "bool", "nullable": True},
    }


def test_columns_builds_timestamp_hints() -> None:
    out = columns(timestamp=("created_at",))
    assert out == {"created_at": {"data_type": "timestamp", "nullable": True}}


def test_columns_builds_mixed_hints() -> None:
    out = columns(
        text=("name",),
        bigint=("count",),
        decimal=("price",),
        boolean=("active",),
        timestamp=("created_at",),
    )
    assert out["name"]["data_type"] == "text"
    assert out["count"]["data_type"] == "bigint"
    assert out["price"]["data_type"] == "decimal"
    assert out["active"]["data_type"] == "bool"
    assert out["created_at"]["data_type"] == "timestamp"


def test_columns_each_entry_is_independent_dict() -> None:
    """Each column hint must be a distinct dict instance (dlt mutates them)."""
    out = columns(text=("a", "b"))
    assert out["a"] is not out["b"]


def test_columns_empty_returns_empty_dict() -> None:
    assert columns() == {}


def test_nullable_column_returns_correct_structure() -> None:
    assert nullable_column("text") == {"data_type": "text", "nullable": True}
    assert nullable_column("bigint") == {"data_type": "bigint", "nullable": True}
