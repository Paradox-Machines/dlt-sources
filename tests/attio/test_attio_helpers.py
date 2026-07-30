"""Unit tests for attio helpers — paginator, value transforms, scope skip."""

from __future__ import annotations

import json
import logging

import pytest
from requests import HTTPError, Response

from paradox_dlt_sources.attio.helpers import (
    AttioRecordOffsetPaginator,
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


# --- AttioRecordOffsetPaginator ---
#
# `POST /v2/objects/{slug}/records/query` is limit/offset paginated and does
# NOT return `pagination.next_cursor`. PAR-1014: a cursor paginator reading a
# field the endpoint never sends stops after page 1, silently truncating
# companies + people to Attio's default page size of 500.


class _Req:
    """Minimal stand-in for `requests.Request` — only `.json` is touched."""

    def __init__(self, body: dict | None = None) -> None:
        self.json = {} if body is None else body


def _full_page(limit: int) -> Response:
    return _response_with_body({"data": [{"id": i} for i in range(limit)]})


def _short_page(n: int) -> Response:
    return _response_with_body({"data": [{"id": i} for i in range(n)]})


def test_paginator_seeds_limit_and_zero_offset_on_first_request():
    p = AttioRecordOffsetPaginator(page_size=1000)
    req = _Req()
    p.update_request(req)
    assert req.json == {"limit": 1000, "offset": 0}


def test_initial_body_carries_limit_and_zero_offset():
    """dlt skips `update_request` on the first call, so page 1 needs seeding."""
    assert AttioRecordOffsetPaginator(page_size=1000).initial_body() == {
        "limit": 1000,
        "offset": 0,
    }


def test_paginator_advances_on_full_page():
    p = AttioRecordOffsetPaginator(page_size=500)
    p.update_state(_full_page(500), data=[{"id": i} for i in range(500)])
    assert p.has_next_page is True

    req = _Req()
    p.update_request(req)
    assert req.json == {"limit": 500, "offset": 500}


def test_paginator_stops_on_short_page():
    p = AttioRecordOffsetPaginator(page_size=500)
    p.update_state(_short_page(499), data=[{"id": i} for i in range(499)])
    assert p.has_next_page is False


def test_paginator_stops_on_empty_page():
    p = AttioRecordOffsetPaginator(page_size=500)
    p.update_state(_short_page(0), data=[])
    assert p.has_next_page is False


def test_paginator_walks_multiple_pages_then_terminates():
    """The regression guard: offset must keep climbing past page 1."""
    p = AttioRecordOffsetPaginator(page_size=100)
    offsets = []

    for page_len in (100, 100, 37):
        req = _Req()
        p.update_request(req)
        offsets.append(req.json["offset"])
        p.update_state(_short_page(page_len), data=[{"id": i} for i in range(page_len)])

    assert offsets == [0, 100, 200]
    assert p.has_next_page is False


def test_paginator_preserves_existing_body_keys():
    p = AttioRecordOffsetPaginator(page_size=500)
    req = _Req({"sorts": [{"direction": "asc"}]})
    p.update_request(req)
    assert req.json == {"sorts": [{"direction": "asc"}], "limit": 500, "offset": 0}


def test_paginator_falls_back_to_response_body_when_data_arg_absent():
    """dlt passes the selected rows, but don't depend on it being supplied."""
    p = AttioRecordOffsetPaginator(page_size=500)
    p.update_state(_full_page(500))
    assert p.has_next_page is True

    p2 = AttioRecordOffsetPaginator(page_size=500)
    p2.update_state(_short_page(3))
    assert p2.has_next_page is False


def test_paginator_rejects_page_size_above_attio_maximum():
    with pytest.raises(ValueError, match="1000"):
        AttioRecordOffsetPaginator(page_size=1001)


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
