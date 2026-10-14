"""In-memory Sage Intacct REST API served through ``responses`` callbacks."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import responses

BASE_URL = "https://api.intacct.com/ia/api/v1"
TOKEN_URL = f"{BASE_URL}/oauth2/token"
QUERY_URL = f"{BASE_URL}/services/core/query"

Record = dict[str, Any]


class FakeSageApi:
    """Serve tokens and query-service pages for ``(entity_id, object)`` datasets.

    The entity a query is answered for is derived from the bearer token, which
    is bound to the ``|entity`` suffix of the username it was issued to.
    """

    def __init__(self, data: Mapping[tuple[str, str], list[Record]] | None = None) -> None:
        self.data: dict[tuple[str, str], list[Record]] = dict(data or {})
        self.token_requests: list[dict[str, Any]] = []
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self._token_entity: dict[str, str] = {}

    def register(self, rsps: responses.RequestsMock) -> None:
        rsps.add_callback(responses.POST, TOKEN_URL, callback=self._token)
        rsps.add_callback(responses.POST, QUERY_URL, callback=self._query)

    def queries_for(self, obj: str) -> list[tuple[str, dict[str, Any]]]:
        return [(entity, body) for entity, body in self.queries if body["object"] == obj]

    def _token(self, request: Any) -> tuple[int, dict[str, str], str]:
        body = json.loads(request.body)
        self.token_requests.append(body)
        token = f"token-{len(self.token_requests)}"
        self._token_entity[token] = body["username"].partition("|")[2]
        payload = {"token_type": "Bearer", "access_token": token, "expires_in": 43200}
        return 200, {}, json.dumps(payload)

    def _query(self, request: Any) -> tuple[int, dict[str, str], str]:
        token = request.headers["Authorization"].removeprefix("Bearer ")
        entity = self._token_entity[token]
        body = json.loads(request.body)
        self.queries.append((entity, body))
        records = self.data.get((entity, body["object"]), [])
        for condition in body.get("filters", []):
            since = condition["$gte"]["audit.modifiedDateTime"]
            records = [r for r in records if r["audit.modifiedDateTime"] >= since]
        start, size = body["start"], body["size"]
        page = records[start - 1 : start - 1 + size]
        next_start = start + size if start - 1 + size < len(records) else None
        meta = {
            "totalCount": len(records),
            "start": start,
            "pageSize": size,
            "next": next_start,
            "previous": None,
        }
        return 200, {}, json.dumps({"ia::result": page, "ia::meta": meta})


def gl_line(key: int, modified: str = "2024-04-01T00:00:00Z", amount: str = "100.00") -> Record:
    return {
        "key": str(key),
        "id": str(key),
        "journalEntry.key": str(key // 10),
        "lineNumber": key % 10,
        "txnType": "debit",
        "txnAmount": amount,
        "baseAmount": amount,
        "entryDate": "2024-03-31",
        "glAccount.id": "4000",
        "dimensions.department.id": "HVAC",
        "audit.modifiedDateTime": modified,
    }
