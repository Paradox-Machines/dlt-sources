"""Unit tests for monday_crm helpers — paginator state machine (no HTTP).

TEMPLATE-OWNED IMPORTS: this header owns every import (``from __future__`` first).
``helpers_test_body`` below is LOGIC ONLY — test functions that reference the
already-imported ``MagicMock`` / ``Request`` / the paginator class(es) and the
``_fake_response`` helper defined here, NEVER a second import block (that block
was the live E402 failure site). Paginator classes are imported via the
``paginator_class_names`` declare-path so a test never smuggles the import (F821).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from requests import Request

from paradox_dlt_sources.monday_crm.helpers import (
    MondayItemsPaginator,
    MondayPagePaginator,
)


def _fake_response(body: dict) -> MagicMock:
    response = MagicMock()
    response.content = b"x"
    response.json.return_value = body
    return response


def test_monday_page_paginator_advances_page() -> None:
    pag = MondayPagePaginator(limit=2)
    data = [{"id": "1"}, {"id": "2"}]
    resp = _fake_response({"data": {"boards": data}})
    pag.update_state(resp, data=data)
    assert pag._has_next_page is True
    assert pag._page == 2


def test_monday_page_paginator_stops_on_short_page() -> None:
    pag = MondayPagePaginator(limit=2)
    data = [{"id": "1"}]
    resp = _fake_response({"data": {"boards": data}})
    pag.update_state(resp, data=data)
    assert pag._has_next_page is False


def test_monday_page_paginator_stops_on_empty_page() -> None:
    pag = MondayPagePaginator(limit=2)
    resp = _fake_response({"data": {"boards": []}})
    pag.update_state(resp, data=[])
    assert pag._has_next_page is False


def test_monday_page_paginator_update_request_injects_page() -> None:
    pag = MondayPagePaginator(limit=2)
    pag._page = 3
    req = MagicMock(spec=Request)
    req.json = {"query": "query { boards { id } }", "variables": {"limit": 2, "page": 1}}
    pag.update_request(req)
    assert req.json["variables"]["page"] == 3
    assert req.json["variables"]["limit"] == 2


def test_monday_items_paginator_advances_on_cursor() -> None:
    pag = MondayItemsPaginator(limit=100)
    body = {"data": {"boards": [{"items_page": {"cursor": "abc123", "items": [{"id": "1"}]}}]}}
    resp = _fake_response(body)
    pag.update_state(resp, data=None)
    assert pag._has_next_page is True
    assert pag._next_cursor == "abc123"


def test_monday_items_paginator_stops_when_no_cursor() -> None:
    pag = MondayItemsPaginator(limit=100)
    body = {"data": {"boards": [{"items_page": {"items": [{"id": "1"}]}}]}}
    resp = _fake_response(body)
    pag.update_state(resp, data=None)
    assert pag._has_next_page is False
    assert pag._next_cursor is None


def test_monday_items_paginator_stops_on_null_cursor() -> None:
    pag = MondayItemsPaginator(limit=100)
    body = {"data": {"boards": [{"items_page": {"cursor": None, "items": [{"id": "1"}]}}]}}
    resp = _fake_response(body)
    pag.update_state(resp, data=None)
    assert pag._has_next_page is False


def test_monday_items_paginator_update_request_injects_cursor() -> None:
    pag = MondayItemsPaginator(limit=100)
    pag._next_cursor = "cur_xyz"
    req = MagicMock(spec=Request)
    req.json = {
        "query": "query { boards { items_page { items { id } } } }",
        "variables": {"limit": 100},
    }
    pag.update_request(req)
    assert req.json["variables"]["cursor"] == "cur_xyz"
    assert req.json["variables"]["limit"] == 100


def test_monday_items_paginator_update_request_removes_cursor_when_none() -> None:
    pag = MondayItemsPaginator(limit=100)
    pag._next_cursor = None
    req = MagicMock(spec=Request)
    req.json = {
        "query": "query { boards { items_page { items { id } } } }",
        "variables": {"limit": 100, "cursor": "old_cursor"},
    }
    pag.update_request(req)
    assert "cursor" not in req.json["variables"]
