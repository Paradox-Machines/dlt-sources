"""Unit tests for stripe helpers — paginator, row transforms, schema hints."""

from __future__ import annotations

import json

import pendulum
import pytest
from requests import Response

from paradox_dlt_sources.stripe.helpers import (
    StripeCursorPaginator,
    coerce_epoch_to_timestamp,
    columns,
    hoist_invoice_subscription,
)


def _response_with_body(body: dict) -> Response:
    """Build a ``requests.Response`` carrying the given JSON body."""
    r = Response()
    r._content = json.dumps(body).encode()
    r.status_code = 200
    return r


# ---------------------------------------------------------------------------
# columns()
# ---------------------------------------------------------------------------


def test_columns_builds_text_and_bigint_hints():
    out = columns(text=("a", "b"), bigint=("c",))
    assert out == {
        "a": {"data_type": "text", "nullable": True},
        "b": {"data_type": "text", "nullable": True},
        "c": {"data_type": "bigint", "nullable": True},
    }


def test_columns_empty():
    assert columns() == {}


def test_columns_text_only():
    out = columns(text=("x",))
    assert out == {"x": {"data_type": "text", "nullable": True}}


# ---------------------------------------------------------------------------
# StripeCursorPaginator
# ---------------------------------------------------------------------------


def test_paginator_advances_when_has_more_and_data():
    p = StripeCursorPaginator()
    data = [{"id": "ch_001"}, {"id": "ch_002"}]
    p.update_state(_response_with_body({"has_more": True}), data=data)
    assert p._has_next_page is True
    assert p._cursor == "ch_002"


def test_paginator_stops_when_has_more_false():
    p = StripeCursorPaginator()
    p.update_state(_response_with_body({"has_more": False}), data=[{"id": "ch_001"}])
    assert p._has_next_page is False


def test_paginator_stops_when_data_empty():
    p = StripeCursorPaginator()
    p.update_state(_response_with_body({"has_more": True}), data=[])
    assert p._has_next_page is False


def test_paginator_writes_cursor_into_next_request_params():
    p = StripeCursorPaginator()
    p.update_state(_response_with_body({"has_more": True}), data=[{"id": "ch_last"}])

    class _Req:
        params = {"created[gt]": 0}

    req = _Req()
    p.update_request(req)
    assert req.params == {"created[gt]": 0, "starting_after": "ch_last"}


def test_paginator_no_op_when_cursor_unset():
    p = StripeCursorPaginator()

    class _Req:
        params = {"limit": 100}

    req = _Req()
    p.update_request(req)
    assert req.params == {"limit": 100}


def test_paginator_params_none_initialised_to_empty_dict():
    """``update_request`` must not crash when ``request.params`` is None."""
    p = StripeCursorPaginator()
    p.update_state(_response_with_body({"has_more": True}), data=[{"id": "ch_x"}])

    class _Req:
        params = None

    req = _Req()
    p.update_request(req)
    assert req.params == {"starting_after": "ch_x"}


# ---------------------------------------------------------------------------
# coerce_epoch_to_timestamp
# ---------------------------------------------------------------------------


def test_coerce_converts_integer_epoch():
    transform = coerce_epoch_to_timestamp("created")
    row = {"id": "ch_001", "created": 1700000000}
    out = transform(row)
    expected = pendulum.from_timestamp(1700000000)
    assert out["created"] == expected


def test_coerce_leaves_non_numeric_unchanged():
    transform = coerce_epoch_to_timestamp("created")
    row = {"id": "ch_001", "created": "2026-01-01T00:00:00Z"}
    out = transform(row)
    assert out["created"] == "2026-01-01T00:00:00Z"


def test_coerce_leaves_missing_field_unchanged():
    transform = coerce_epoch_to_timestamp("created")
    row = {"id": "ch_001"}
    out = transform(row)
    assert out == {"id": "ch_001"}


def test_coerce_does_not_mutate_original():
    transform = coerce_epoch_to_timestamp("created")
    original = {"id": "ch_001", "created": 1700000000}
    _ = transform(original)
    assert isinstance(original["created"], int)


def test_coerce_handles_bool_without_converting():
    """Booleans are ints in Python; ensure we don't coerce True/False."""
    transform = coerce_epoch_to_timestamp("created")
    row = {"created": True}
    out = transform(row)
    assert out["created"] is True


# ---------------------------------------------------------------------------
# hoist_invoice_subscription
# ---------------------------------------------------------------------------


def test_hoist_returns_existing_subscription_unchanged():
    row = {"id": "in_001", "subscription": "sub_existing"}
    out = hoist_invoice_subscription(row)
    assert out["subscription"] == "sub_existing"


def test_hoist_extracts_subscription_from_parent():
    row = {
        "id": "in_002",
        "subscription": None,
        "parent": {
            "subscription_details": {
                "subscription": "sub_from_parent",
            }
        },
    }
    out = hoist_invoice_subscription(row)
    assert out["subscription"] == "sub_from_parent"


def test_hoist_returns_row_unchanged_when_no_parent():
    row = {"id": "in_003", "subscription": None}
    out = hoist_invoice_subscription(row)
    assert out["subscription"] is None


def test_hoist_returns_row_unchanged_when_nested_sub_missing():
    row = {
        "id": "in_004",
        "subscription": None,
        "parent": {"subscription_details": {}},
    }
    out = hoist_invoice_subscription(row)
    assert out["subscription"] is None


@pytest.mark.parametrize("missing_key", ["parent", "subscription_details"])
def test_hoist_tolerates_absent_intermediate_keys(missing_key: str):
    row: dict = {"id": "in_005", "subscription": None}
    if missing_key == "subscription_details":
        row["parent"] = {}
    out = hoist_invoice_subscription(row)
    assert out["subscription"] is None
