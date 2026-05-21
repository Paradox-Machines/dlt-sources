"""Unit tests for Notion source helpers — paginators and schema hints."""

from __future__ import annotations

import pytest
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

from paradox_dlt_sources.notion.helpers import (
    children_paginator,
    columns,
    comments_paginator,
    search_paginator,
    users_paginator,
)

# ---------------------------------------------------------------------------
# columns()
# ---------------------------------------------------------------------------


def test_columns_builds_text_hints() -> None:
    out = columns(text=("id", "name"))
    assert out == {
        "id": {"data_type": "text", "nullable": True},
        "name": {"data_type": "text", "nullable": True},
    }


def test_columns_builds_timestamp_hints() -> None:
    out = columns(timestamp=("created_time",))
    assert out == {"created_time": {"data_type": "timestamp", "nullable": True}}


def test_columns_builds_bool_hints() -> None:
    out = columns(bool_=("archived",))
    assert out == {"archived": {"data_type": "bool", "nullable": True}}


def test_columns_builds_json_hints() -> None:
    out = columns(json_=("properties",))
    assert out == {"properties": {"data_type": "json", "nullable": True}}


def test_columns_builds_mixed_hints() -> None:
    out = columns(
        text=("id", "url"),
        timestamp=("created_time",),
        bool_=("archived",),
        json_=("properties",),
    )
    assert len(out) == 5
    assert out["id"] == {"data_type": "text", "nullable": True}
    assert out["created_time"] == {"data_type": "timestamp", "nullable": True}
    assert out["archived"] == {"data_type": "bool", "nullable": True}
    assert out["properties"] == {"data_type": "json", "nullable": True}


def test_columns_empty_returns_empty_dict() -> None:
    assert columns() == {}


# ---------------------------------------------------------------------------
# Paginators — verify they are JSONResponseCursorPaginator instances with
# the correct cursor field names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("factory", "expected_cursor_param", "expected_cursor_body_path"),
    [
        (users_paginator, "start_cursor", None),
        (children_paginator, "start_cursor", None),
        (comments_paginator, "start_cursor", None),
    ],
)
def test_query_param_paginators_use_start_cursor(
    factory: object,
    expected_cursor_param: str,
    expected_cursor_body_path: str | None,
) -> None:
    p = factory()  # type: ignore[operator]
    assert isinstance(p, JSONResponseCursorPaginator)
    assert p.cursor_param == expected_cursor_param  # type: ignore[union-attr]


def test_search_paginator_uses_cursor_body_path() -> None:
    """POST /v1/search requires the cursor in the request body, not a query param."""
    p = search_paginator()
    assert isinstance(p, JSONResponseCursorPaginator)
    # cursor_body_path puts the cursor in the JSON body for POST requests
    assert p.cursor_body_path == "start_cursor"  # type: ignore[union-attr]
    # must NOT have a query-param cursor (would conflict with body cursor)
    assert p.cursor_param is None  # type: ignore[union-attr]


def test_all_paginators_use_has_more_path() -> None:
    """All Notion paginators read `has_more` from the response envelope.

    dlt may wrap the jsonpath string in a descriptor object whose str()
    returns the plain string value.
    """
    for factory in (users_paginator, search_paginator, children_paginator, comments_paginator):
        p = factory()
        assert str(p.has_more_path) == "has_more"  # type: ignore[union-attr]


def test_all_paginators_read_next_cursor_from_response() -> None:
    """All Notion paginators extract the continuation token from `next_cursor`.

    dlt may wrap the jsonpath string in a descriptor object whose str()
    returns the plain string value.
    """
    for factory in (users_paginator, search_paginator, children_paginator, comments_paginator):
        p = factory()
        assert str(p.cursor_path) == "next_cursor"  # type: ignore[union-attr]
