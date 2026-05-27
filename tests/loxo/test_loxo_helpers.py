"""Unit tests for loxo helpers — scroll-id paginator + base_url construction."""

from __future__ import annotations

from unittest.mock import MagicMock

from requests import Request

from paradox_dlt_sources.loxo.helpers import LoxoScrollIdPaginator, _base_url


def _fake_response(body: dict) -> MagicMock:
    response = MagicMock()
    response.content = b"x"
    response.json.return_value = body
    return response


# --- LoxoScrollIdPaginator ---


def test_scroll_id_paginator_replaces_param_across_pages() -> None:
    paginator = LoxoScrollIdPaginator()
    request = Request(
        method="GET",
        url="https://example.loxo.co/api/example-agency/people",
        params={"per_page": 100},
    )

    paginator.update_state(_fake_response({"scroll_id": "abc"}), data=[{"id": 1}])
    paginator.update_request(request)
    paginator.update_state(_fake_response({"scroll_id": "def"}), data=[{"id": 2}])
    paginator.update_request(request)

    assert request.params["scroll_id"] == "def"
    assert request.params["per_page"] == 100
    # Dict replacement, not string accumulation.
    assert sum(1 for k in request.params if k == "scroll_id") == 1
    assert request.url.count("scroll_id=") == 0


def test_scroll_id_paginator_stops_on_missing_scroll_id() -> None:
    paginator = LoxoScrollIdPaginator()
    paginator.update_state(_fake_response({"people": []}), data=[{"id": 1}])
    assert paginator._has_next_page is False


def test_scroll_id_paginator_stops_on_empty_page() -> None:
    paginator = LoxoScrollIdPaginator()
    paginator.update_state(_fake_response({"scroll_id": "abc"}), data=[])
    assert paginator._has_next_page is False


# --- _base_url ---


def test_base_url_constructs_default_path() -> None:
    assert _base_url("app.loxo.co", "acme") == "https://app.loxo.co/api/acme"


def test_base_url_honors_env_override(monkeypatch) -> None:
    monkeypatch.setenv("LOXO_API_BASE_URL", "https://custom.example.com/api/acme/")
    # Trailing slash should be stripped.
    assert _base_url("ignored", "ignored") == "https://custom.example.com/api/acme"
