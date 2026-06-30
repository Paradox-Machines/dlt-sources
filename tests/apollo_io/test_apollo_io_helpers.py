"""Unit tests for apollo_io helpers — paginator state machine (no HTTP).

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

from paradox_dlt_sources.apollo_io.helpers import ApolloIoPagePaginator


def _fake_response(body: dict) -> MagicMock:
    response = MagicMock()
    response.content = b"x"
    response.json.return_value = body
    return response


def test_apollo_io_page_paginator_advances() -> None:
    pag = ApolloIoPagePaginator(per_page=10)
    assert pag._has_next_page is True
    assert pag._page == 1

    resp = _fake_response({"email_accounts": [{"id": "ea1"}]})
    pag.update_state(resp, data=[{"id": "ea1"}])
    assert pag._page == 2
    assert pag._has_next_page is True

    req = Request(method="GET", url="https://api.apollo.io/v1/email_accounts")
    pag.update_request(req)
    assert req.params["page"] == 2
    assert req.params["per_page"] == 10


def test_apollo_io_page_paginator_terminal_empty_list() -> None:
    pag = ApolloIoPagePaginator(per_page=100)
    resp = _fake_response({"email_accounts": []})
    pag.update_state(resp, data=[])
    assert pag._has_next_page is False


def test_apollo_io_page_paginator_terminal_none_data() -> None:
    pag = ApolloIoPagePaginator(per_page=100)
    resp = _fake_response({})
    pag.update_state(resp, data=None)
    assert pag._has_next_page is False


def test_apollo_io_page_paginator_sets_params_on_first_request() -> None:
    pag = ApolloIoPagePaginator(per_page=50)
    req = Request(method="GET", url="https://api.apollo.io/v1/labels")
    pag.update_request(req)
    assert req.params["page"] == 1
    assert req.params["per_page"] == 50


def test_apollo_io_page_paginator_multiple_pages() -> None:
    pag = ApolloIoPagePaginator(per_page=2)

    resp1 = _fake_response({"labels": [{"id": "lb1"}, {"id": "lb2"}]})
    pag.update_state(resp1, data=[{"id": "lb1"}, {"id": "lb2"}])
    assert pag._page == 2

    req = Request(method="GET", url="https://api.apollo.io/v1/labels")
    pag.update_request(req)
    assert req.params["page"] == 2

    resp2 = _fake_response({"labels": []})
    pag.update_state(resp2, data=[])
    assert pag._has_next_page is False
