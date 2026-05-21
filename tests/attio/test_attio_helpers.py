"""Unit tests for attio helpers — paginator, value transforms, scope skip."""

from __future__ import annotations

import json
import logging

import pytest
from requests import HTTPError, Response

from paradox_dlt_sources.attio.helpers import (
    AttioRecordCursorPaginator,
    active_scalar,
    columns,
    promote_active_values,
    skip_on_forbidden,
)


def _response_with_body(body: dict) -> Response:
    """Build a `requests.Response` carrying the given JSON body."""
    r = Response()
    r._content = json.dumps(body).encode()
    r.status_code = 200
    return r


# --- columns() ---


def test_columns_builds_text_and_bigint_hints():
    out = columns(text=("a", "b"), bigint=("c",))
    assert out == {
        "a": {"data_type": "text", "nullable": True},
        "b": {"data_type": "text", "nullable": True},
        "c": {"data_type": "bigint", "nullable": True},
    }


def test_columns_empty():
    assert columns() == {}


# --- AttioRecordCursorPaginator ---


def test_paginator_advances_when_next_cursor_present():
    p = AttioRecordCursorPaginator()
    p.update_state(_response_with_body({"pagination": {"next_cursor": "abc"}}))
    assert p._has_next_page is True
    assert p._cursor == "abc"


def test_paginator_stops_when_next_cursor_absent():
    p = AttioRecordCursorPaginator()
    p.update_state(_response_with_body({"pagination": {}}))
    assert p._has_next_page is False


def test_paginator_writes_cursor_into_next_request_body():
    p = AttioRecordCursorPaginator()
    p.update_state(_response_with_body({"pagination": {"next_cursor": "xyz"}}))

    class _Req:
        json = {"existing": 1}

    req = _Req()
    p.update_request(req)
    assert req.json == {"existing": 1, "cursor": "xyz"}


def test_paginator_no_op_when_cursor_unset():
    p = AttioRecordCursorPaginator()

    class _Req:
        json = {"existing": 1}

    req = _Req()
    p.update_request(req)
    assert req.json == {"existing": 1}


# --- active_scalar ---


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ({"value": "x"}, "x"),
        ({"email_address": "a@b.c"}, "a@b.c"),
        ({"domain": "acme.com"}, "acme.com"),
        ({"full_name": "Jane Doe"}, "Jane Doe"),
        ({"original_phone_number": "+15551234"}, "+15551234"),
        ({"formatted_address": "1 Main St"}, "1 Main St"),
        ({"currency_value": 12345}, 12345),
        ({"referenced_actor_id": "actor-1"}, "actor-1"),
        ({"target_record_id": "rec-99"}, "rec-99"),
        ({"status": {"title": "Won"}}, "Won"),
        ({}, None),
    ],
)
def test_active_scalar_extracts_attribute_specific_field(entry, expected):
    assert active_scalar(entry) == expected


# --- promote_active_values ---


def test_promote_active_values_hoists_current_entry_scalar():
    row = {
        "record_id": "rec-1",
        "values": {
            "name": [{"active_from": "2026-01-01", "active_until": None, "value": "Acme"}],
            "domains": [{"active_from": "2026-01-01", "active_until": None, "domain": "acme.com"}],
        },
    }
    out = promote_active_values(row)
    assert out["name"] == "Acme"
    assert out["domains"] == "acme.com"


def test_promote_active_values_ignores_inactive_entries():
    row = {
        "values": {
            "name": [
                {"active_from": "2026-01-01", "active_until": "2026-06-01", "value": "Old"},
            ],
        },
    }
    out = promote_active_values(row)
    assert "name" not in out


def test_promote_active_values_does_not_overwrite_existing_top_level():
    row = {
        "name": "preset",
        "values": {
            "name": [{"active_from": "2026-01-01", "active_until": None, "value": "from-values"}],
        },
    }
    out = promote_active_values(row)
    assert out["name"] == "preset"


def test_promote_active_values_no_values_key_returns_unchanged():
    row = {"record_id": "rec-1"}
    assert promote_active_values(row) == {"record_id": "rec-1"}


# --- skip_on_forbidden ---


def test_skip_on_forbidden_swallows_403_and_logs(caplog):
    def _gen():
        yield 1
        resp = Response()
        resp.status_code = 403
        raise HTTPError(response=resp)

    with caplog.at_level(logging.WARNING):
        out = list(skip_on_forbidden("companies", "record_permission:read", _gen()))
    assert out == [1]
    assert any("403 Forbidden" in m for m in caplog.messages)
    assert any("record_permission:read" in m for m in caplog.messages)


def test_skip_on_forbidden_reraises_non_403():
    def _gen():
        resp = Response()
        resp.status_code = 500
        raise HTTPError(response=resp)

    with pytest.raises(HTTPError):
        list(skip_on_forbidden("companies", "scope", _gen()))


def test_skip_on_forbidden_reraises_http_error_without_response():
    def _gen():
        raise HTTPError()  # no response attached

    with pytest.raises(HTTPError):
        list(skip_on_forbidden("companies", "scope", _gen()))
