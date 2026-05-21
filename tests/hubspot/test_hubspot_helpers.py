"""Unit tests for hubspot helpers — paginators and schema hints."""

from __future__ import annotations

import json

import pytest
from requests import PreparedRequest, Response

from paradox_dlt_sources.hubspot import _OBJECT_COLUMNS, _flatten_engagement
from paradox_dlt_sources.hubspot.helpers import (
    EngagementsOffsetPaginator,
    columns,
)


def _response_with_body(body: dict) -> Response:  # type: ignore[type-arg]
    """Build a ``requests.Response`` carrying the given JSON body."""
    r = Response()
    r._content = json.dumps(body).encode()
    r.status_code = 200
    return r


def _prepared_request(params: dict | None = None) -> PreparedRequest:  # type: ignore[type-arg]
    req = PreparedRequest()
    req.params = params or {}  # type: ignore[assignment]
    return req


# --- columns() ---


def test_columns_builds_text_hints() -> None:
    out = columns(text=("a", "b"))
    assert out == {
        "a": {"data_type": "text", "nullable": True},
        "b": {"data_type": "text", "nullable": True},
    }


def test_columns_builds_bigint_hints() -> None:
    out = columns(bigint=("x",))
    assert out == {"x": {"data_type": "bigint", "nullable": True}}


def test_columns_builds_mixed_hints() -> None:
    out = columns(text=("a",), bigint=("b",))
    assert out == {
        "a": {"data_type": "text", "nullable": True},
        "b": {"data_type": "bigint", "nullable": True},
    }


def test_columns_empty() -> None:
    assert columns() == {}


def test_columns_each_value_is_independent_dict() -> None:
    """dlt mutates column hint dicts (assigns ``name``); ensure no aliasing."""
    out = columns(text=("p", "q"))
    assert out["p"] is not out["q"]


# --- EngagementsOffsetPaginator ---


def test_engagements_paginator_advances_when_has_more() -> None:
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"hasMore": True, "offset": 5000, "results": []}))
    assert p._has_next_page is True
    assert p._offset == 5000


def test_engagements_paginator_stops_when_has_more_false() -> None:
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"hasMore": False, "offset": 5002, "results": []}))
    assert p._has_next_page is False


def test_engagements_paginator_stops_when_has_more_missing() -> None:
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"results": []}))
    assert p._has_next_page is False


def test_engagements_paginator_stops_when_offset_missing_despite_has_more() -> None:
    """Guard: hasMore=True but no offset field means we cannot advance safely."""
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"hasMore": True, "results": []}))
    assert p._has_next_page is False


def test_engagements_paginator_writes_offset_into_request_params() -> None:
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"hasMore": True, "offset": 5000, "results": []}))

    req = _prepared_request(params={"limit": "250"})
    p.update_request(req)
    assert req.params == {"limit": "250", "offset": 5000}  # type: ignore[comparison-overlap]


def test_engagements_paginator_no_op_when_offset_unset() -> None:
    p = EngagementsOffsetPaginator()
    req = _prepared_request(params={"limit": "250"})
    p.update_request(req)
    # params unchanged — offset not injected before first update_state
    assert "offset" not in (req.params or {})


def test_engagements_paginator_coerces_offset_to_int() -> None:
    """Server may return offset as a string in some edge cases."""
    p = EngagementsOffsetPaginator()
    p.update_state(_response_with_body({"hasMore": True, "offset": "7500", "results": []}))
    assert p._offset == 7500
    assert isinstance(p._offset, int)


# --- _flatten_engagement ---


def test_flatten_engagement_lifts_engagement_fields() -> None:
    item = {
        "engagement": {"id": 101, "type": "NOTE", "timestamp": 1712570400000},
        "associations": {
            "contactIds": [1001, 1002],
            "companyIds": [2001],
            "dealIds": [],
        },
    }
    out = _flatten_engagement(item)
    assert out["id"] == 101
    assert out["type"] == "NOTE"
    assert json.loads(out["associations__contact_ids"]) == [1001, 1002]
    assert json.loads(out["associations__company_ids"]) == [2001]
    assert json.loads(out["associations__deal_ids"]) == []


def test_flatten_engagement_handles_missing_associations() -> None:
    item = {"engagement": {"id": 200, "type": "CALL"}}
    out = _flatten_engagement(item)
    assert out["id"] == 200
    assert json.loads(out["associations__contact_ids"]) == []
    assert json.loads(out["associations__company_ids"]) == []
    assert json.loads(out["associations__deal_ids"]) == []


def test_flatten_engagement_handles_empty_item() -> None:
    out = _flatten_engagement({})
    assert json.loads(out["associations__contact_ids"]) == []


@pytest.mark.parametrize(
    ("obj_name", "expected_prefix"),
    [
        ("companies", "properties__name"),
        ("contacts", "properties__email"),
        ("deals", "properties__dealname"),
    ],
)
def test_object_columns_use_properties_prefix(obj_name: str, expected_prefix: str) -> None:
    assert expected_prefix in _OBJECT_COLUMNS[obj_name]
    hint = _OBJECT_COLUMNS[obj_name][expected_prefix]
    assert hint["data_type"] == "text"
    assert hint["nullable"] is True


def test_engagements_columns_have_association_fields() -> None:
    for col in ("associations__contact_ids", "associations__company_ids", "associations__deal_ids"):
        assert col in _OBJECT_COLUMNS["engagements"]


def test_deal_pipelines_columns_have_stages() -> None:
    assert "stages" in _OBJECT_COLUMNS["deal_pipelines"]
