"""Unit tests for pipedrive helpers — paginator, row transforms, schema hints."""

from __future__ import annotations

import json
from typing import Any

from requests import Request, Response

from paradox_dlt_sources.pipedrive.helpers import (
    PipedrivePaginator,
    columns,
    flatten_deal_user_refs,
    flatten_person_contact_arrays,
    flatten_person_owner_ref,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _response_with_body(body: dict[str, Any], status: int = 200) -> Response:
    """Build a `requests.Response` carrying the given JSON body."""
    r = Response()
    r._content = json.dumps(body).encode()
    r.status_code = status
    return r


def _request_with_params(params: dict[str, Any] | None = None) -> Request:
    """Build a `requests.Request` with the given query params dict.

    PipedrivePaginator.update_request expects `requests.Request` (the
    pre-send object, not `PreparedRequest`), matching the dlt BasePaginator
    contract.
    """
    req = Request(method="GET", url="https://api.pipedrive.com/v1/persons")
    req.params = params or {}
    return req


# ---------------------------------------------------------------------------
# columns()
# ---------------------------------------------------------------------------


def test_columns_builds_text_hints() -> None:
    out = columns(text=("name", "email"))
    assert out == {
        "name": {"data_type": "text", "nullable": True},
        "email": {"data_type": "text", "nullable": True},
    }


def test_columns_builds_bigint_hints() -> None:
    out = columns(bigint=("owner_id",))
    assert out == {"owner_id": {"data_type": "bigint", "nullable": True}}


def test_columns_mixed_text_and_bigint() -> None:
    out = columns(text=("name",), bigint=("owner_id",))
    assert out["name"]["data_type"] == "text"
    assert out["owner_id"]["data_type"] == "bigint"


def test_columns_empty() -> None:
    assert columns() == {}


def test_columns_each_hint_is_distinct_dict_instance() -> None:
    # dlt mutates column hint dicts (assigns 'name'). Reusing one instance
    # would silently merge columns under a single name.
    out = columns(text=("a", "b"))
    assert out["a"] is not out["b"]


# ---------------------------------------------------------------------------
# PipedrivePaginator
# ---------------------------------------------------------------------------


def test_paginator_advances_when_more_items() -> None:
    p = PipedrivePaginator()
    body: dict[str, Any] = {
        "data": [],
        "additional_data": {
            "pagination": {
                "start": 0,
                "limit": 100,
                "more_items_in_collection": True,
                "next_start": 100,
            }
        },
    }
    p.update_state(_response_with_body(body))
    assert p._has_next_page is True
    assert p._next_start == 100


def test_paginator_stops_when_no_more_items() -> None:
    p = PipedrivePaginator()
    body: dict[str, Any] = {
        "data": [],
        "additional_data": {
            "pagination": {"start": 0, "limit": 100, "more_items_in_collection": False}
        },
    }
    p.update_state(_response_with_body(body))
    assert p._has_next_page is False


def test_paginator_stops_when_additional_data_missing() -> None:
    p = PipedrivePaginator()
    p.update_state(_response_with_body({"data": []}))
    assert p._has_next_page is False


def test_paginator_stops_when_next_start_absent_despite_more_items() -> None:
    # Edge case: server says more_items=True but omits next_start.
    p = PipedrivePaginator()
    body: dict[str, Any] = {"additional_data": {"pagination": {"more_items_in_collection": True}}}
    p.update_state(_response_with_body(body))
    assert p._has_next_page is False


def test_paginator_writes_start_into_request_params() -> None:
    p = PipedrivePaginator()
    body: dict[str, Any] = {
        "additional_data": {
            "pagination": {
                "more_items_in_collection": True,
                "next_start": 200,
            }
        }
    }
    p.update_state(_response_with_body(body))

    req = _request_with_params({"limit": "100", "start": "0"})
    p.update_request(req)
    assert req.params["start"] == 200


def test_paginator_no_op_when_next_start_unset() -> None:
    p = PipedrivePaginator()
    req = _request_with_params({"limit": "100", "start": "0"})
    p.update_request(req)
    # params unchanged
    assert req.params["start"] == "0"


# ---------------------------------------------------------------------------
# flatten_person_contact_arrays
# ---------------------------------------------------------------------------


def test_flatten_person_contact_arrays_extracts_primary_values() -> None:
    row = {
        "id": 1,
        "email": [{"label": "work", "value": "a@b.com", "primary": True}],
        "phone": [{"label": "mobile", "value": "+1555", "primary": True}],
    }
    out = flatten_person_contact_arrays(row)
    assert out["email_primary"] == "a@b.com"
    assert out["phone_primary"] == "+1555"


def test_flatten_person_contact_arrays_empty_list_yields_none() -> None:
    row: dict[str, Any] = {"id": 2, "email": [], "phone": []}
    out = flatten_person_contact_arrays(row)
    assert out["email_primary"] is None
    assert out["phone_primary"] is None


def test_flatten_person_contact_arrays_missing_field_yields_none() -> None:
    row: dict[str, Any] = {"id": 3}
    out = flatten_person_contact_arrays(row)
    assert out["email_primary"] is None
    assert out["phone_primary"] is None


def test_flatten_person_contact_arrays_preserves_original_fields() -> None:
    row: dict[str, Any] = {"id": 4, "name": "Test", "email": [], "phone": []}
    out = flatten_person_contact_arrays(row)
    assert out["name"] == "Test"
    assert out["id"] == 4


# ---------------------------------------------------------------------------
# flatten_deal_user_refs
# ---------------------------------------------------------------------------


def test_flatten_deal_user_refs_extracts_id_from_nested_object() -> None:
    row = {
        "id": 101,
        "user_id": {"id": 10, "name": "Alice"},
        "creator_user_id": {"id": 11, "name": "Bob"},
    }
    out = flatten_deal_user_refs(row)
    assert out["user_id"] == 10
    assert out["creator_user_id"] == 11


def test_flatten_deal_user_refs_passes_through_scalar_ids() -> None:
    row: dict[str, Any] = {"id": 102, "user_id": 10, "creator_user_id": 11}
    out = flatten_deal_user_refs(row)
    assert out["user_id"] == 10
    assert out["creator_user_id"] == 11


def test_flatten_deal_user_refs_handles_none() -> None:
    row: dict[str, Any] = {"id": 103, "user_id": None, "creator_user_id": None}
    out = flatten_deal_user_refs(row)
    assert out["user_id"] is None
    assert out["creator_user_id"] is None


# ---------------------------------------------------------------------------
# flatten_person_owner_ref
# ---------------------------------------------------------------------------


def test_flatten_person_owner_ref_extracts_id() -> None:
    row: dict[str, Any] = {"id": 1, "owner_id": {"id": 10, "name": "Owner"}}
    out = flatten_person_owner_ref(row)
    assert out["owner_id"] == 10


def test_flatten_person_owner_ref_passes_through_scalar() -> None:
    row: dict[str, Any] = {"id": 2, "owner_id": 10}
    out = flatten_person_owner_ref(row)
    assert out["owner_id"] == 10
