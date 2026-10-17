"""A path-routed fake of the SKY API for `responses`, recording every request."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlparse

import responses

API = "https://api.sky.blackbaud.com"
TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"

Handler = Callable[[dict[str, list[str]]], Any]

_EMPTY_BY_PATH = {
    "/gift-batch/v1/giftbatches": {"count": 0, "giftbatches": []},
    "/customfield/customfieldcategories": {"count": 0, "custom_field_categories": []},
    "/constituent/v1/currencyconfiguration": {"currency_code": "USD", "currency_symbol": "$"},
}


class FakeSky:
    """Register on a ``responses`` mock; ``routes`` maps a path to a handler or body."""

    def __init__(self, rsps: responses.RequestsMock) -> None:
        self.routes: dict[str, Any] = {}
        self.calls: list[tuple[str, dict[str, list[str]]]] = []
        self.token_requests: list[dict[str, list[str]]] = []
        self.token_body: dict[str, Any] = {
            "access_token": "tok",
            "expires_in": 3599,
            "refresh_token": "rtoken",
            "environment_id": "p-env",
        }
        rsps.add_callback(responses.POST, TOKEN_URL, callback=self._token)
        rsps.add_callback(responses.GET, re.compile(re.escape(API) + ".*"), callback=self._get)

    def _token(self, request: Any) -> tuple[int, dict[str, str], str]:
        self.token_requests.append(parse_qs(request.body))
        return 200, {}, json.dumps(self.token_body)

    def _get(self, request: Any) -> tuple[int, dict[str, str], str]:
        url = urlparse(request.url)
        params = parse_qs(url.query)
        self.calls.append((url.path, params))
        route = self.routes.get(url.path)
        if callable(route):
            result = route(params)
        elif route is not None:
            result = route
        else:
            result = _EMPTY_BY_PATH.get(url.path, {"count": 0, "value": []})
        if (
            isinstance(result, dict)
            and int(params.get("offset", ["0"])[0]) > 0
            and "next_link" not in result
        ):
            # Offset-paged endpoints: static bodies are a single page.
            result = {**result, **{k: [] for k, v in result.items() if isinstance(v, list)}}
        if isinstance(result, tuple):
            status, body = result
            return status, {}, json.dumps(body)
        return 200, {}, json.dumps(result)

    def params_for(self, path: str) -> list[dict[str, list[str]]]:
        """Query strings of every call to ``path``."""
        return [p for c, p in self.calls if c == path]


def pages(*batches: list[dict[str, Any]], path: str) -> Handler:
    """A `next_link` + `sort_token` handler serving ``batches`` in order."""

    def handler(params: dict[str, list[str]]) -> dict[str, Any]:
        index = int(params.get("sort_token", ["0"])[0])
        body: dict[str, Any] = {
            "count": sum(len(b) for b in batches),
            "value": batches[index] if index < len(batches) else [],
        }
        if index + 1 < len(batches):
            body["next_link"] = f"{API}{path}?sort_token={index + 1}&limit=5000"
        return body

    return handler
