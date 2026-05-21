"""QuickBooks Online source helpers.

Contains:
- RotatingRefreshTokenAuth  — OAuth 2.0 refresh-token grant with Intuit's
  rotating-refresh-token semantics.
- QuickBooksQueryPaginator  — STARTPOSITION-based paginator for QBO's SQL
  query layer.
- _coerce_metadata_last_updated_time  — row transform for incremental cursor.
- columns / nullable_column  — schema-hint builders (mirror attio pattern).
- _build_query  — QBO SQL query string builder.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Callable
from typing import Any

import pendulum
import requests as _requests
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import PreparedRequest, Request, Response
from requests.auth import AuthBase

from .settings import MAX_RESULTS

# ---------------------------------------------------------------------------
# Safety margin: refresh the access token this many seconds before the
# server-stated expiry to avoid race conditions against QBO's 60-minute TTL.
# ---------------------------------------------------------------------------
_REFRESH_SAFETY_SECONDS = 60

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Schema hint builders (same pattern as attio/helpers.py)
# ---------------------------------------------------------------------------


def nullable_column(data_type: str) -> dict[str, Any]:
    """Return a single nullable column hint dict."""
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=...)`` map of nullable hints.

    dlt drops all-NULL columns from the load schema, breaking downstream
    consumers that reference them.  Up-front hints guarantee the column
    exists regardless of data shape.  Each column gets its own dict
    instance — dlt mutates these (assigns ``name``) and sharing a single
    dict across columns silently merges them.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    return out


# ---------------------------------------------------------------------------
# OAuth 2.0 with rotating refresh tokens
# ---------------------------------------------------------------------------


class RotatingRefreshTokenAuth(AuthBase):
    """OAuth 2.0 refresh-token grant for Intuit/QuickBooks.

    Intuit may emit a **new** ``refresh_token`` on *any* token-refresh
    response (rotation is not guaranteed on every call but must be
    handled on every call).  The old refresh token expires within ~24 h.

    Rotation detection:
        After each successful ``/oauth2/v1/tokens/bearer`` call, if the
        response body contains a ``refresh_token`` value that **differs**
        from the one used in the request, the new value is stored and
        ``on_token_rotation(new_refresh_token)`` is called immediately.
        Callers should persist the new token to their secrets backend
        before the pipeline run ends.

    Intuit auth wire format:
        Client credentials are sent via HTTP Basic auth
        (``Authorization: Basic base64(client_id:client_secret)``), NOT
        in the form body.  Placing them in the body yields
        ``invalid_client``.

    Args:
        token_url:        Intuit token endpoint.
        refresh_token:    The current OAuth refresh token.
        client_id:        Intuit app client ID.
        client_secret:    Intuit app client secret.
        on_token_rotation: Callable invoked with the **new** refresh token
                          when Intuit rotates it.  Pass a no-op lambda
                          (or ``None`` — the factory wraps it) if you do
                          not need write-back.
    """

    def __init__(
        self,
        *,
        token_url: str,
        refresh_token: str,
        client_id: str,
        client_secret: str,
        on_token_rotation: Callable[[str], None],
    ) -> None:
        self._token_url = token_url
        self._refresh_token = refresh_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._on_token_rotation = on_token_rotation
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    # ------------------------------------------------------------------
    # AuthBase protocol
    # ------------------------------------------------------------------

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self._access_token is None or time.time() >= self._expires_at - _REFRESH_SAFETY_SECONDS:
            self._refresh_access_token()
        request.headers["Authorization"] = f"Bearer {self._access_token}"
        return request

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _refresh_access_token(self) -> None:
        """Exchange the current refresh token for a fresh access token.

        Persists a new refresh token via ``on_token_rotation`` if Intuit
        rotated it in this response.
        """
        creds = f"{self._client_id}:{self._client_secret}".encode()
        basic = base64.b64encode(creds).decode()
        response = _requests.post(
            self._token_url,
            headers={
                "Authorization": f"Basic {basic}",
                "Accept": "application/json",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            },
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = body["access_token"]
        self._expires_at = time.time() + body["expires_in"]
        new_refresh = body.get("refresh_token")
        if new_refresh is not None and new_refresh != self._refresh_token:
            self._refresh_token = new_refresh
            self._on_token_rotation(new_refresh)


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------


def build_query(
    *,
    entity: str,
    start_value_iso: str | None,
    start_position: int,
) -> str:
    """Build a QBO SQL query string for one page of *entity*.

    ``start_value_iso`` is the incremental cursor floor expressed as an
    ISO-8601 string.  When ``None``, no WHERE clause is added (full load /
    replace resources).  Results are always ``ORDER BY
    MetaData.LastUpdatedTime`` so paginated reads are deterministic.
    """
    where = (
        f"WHERE MetaData.LastUpdatedTime > '{start_value_iso}' "
        if start_value_iso is not None
        else ""
    )
    return (
        f"SELECT * FROM {entity} "
        f"{where}"
        f"ORDER BY MetaData.LastUpdatedTime "
        f"STARTPOSITION {start_position} MAXRESULTS {MAX_RESULTS}"
    )


# ---------------------------------------------------------------------------
# Paginator
# ---------------------------------------------------------------------------


class QuickBooksQueryPaginator(BasePaginator):
    """Paginate ``/v3/company/{realmId}/query`` by advancing STARTPOSITION.

    QBO returns one entity-named array per response
    (``QueryResponse.<Entity>``).  A "short page" (fewer rows than
    ``MAX_RESULTS``) signals end-of-results.

    Args:
        entity:     QBO entity class name, e.g. ``"Invoice"``.
        base_query: The initial query string (STARTPOSITION 1).  On each
                    subsequent request the paginator strips the trailing
                    ``STARTPOSITION … MAXRESULTS …`` clause and re-appends
                    with the advanced position.
    """

    def __init__(self, *, entity: str, base_query: str) -> None:
        super().__init__()
        self._entity = entity
        self._base_query = base_query
        self._start_position = 1
        self._has_next_page = False

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        body = response.json() or {}
        qr = body.get("QueryResponse") or {}
        rows = qr.get(self._entity) or []
        if len(rows) >= MAX_RESULTS:
            self._start_position += MAX_RESULTS
            self._has_next_page = True
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        """Re-write the ``query`` request param with the advanced STARTPOSITION.

        dlt's REST client passes a ``requests.Request`` to this method; the
        ``query`` QBO SQL string lives in ``request.params`` dict and is
        encoded into the URL when the request is prepared.
        """
        # Strip the trailing " STARTPOSITION … MAXRESULTS …" from the base
        # and re-append.  Cheaper than re-parsing the SQL.
        base = self._base_query.split(" STARTPOSITION ")[0]
        qbo_query = f"{base} STARTPOSITION {self._start_position} MAXRESULTS {MAX_RESULTS}"
        # request.params may be a dict or None; normalise to dict before writing.
        if not isinstance(request.params, dict):
            request.params = {}
        request.params["query"] = qbo_query


# ---------------------------------------------------------------------------
# Row transform
# ---------------------------------------------------------------------------


def coerce_metadata_last_updated_time(row: Row) -> Row:
    """Coerce ``MetaData.LastUpdatedTime`` from ISO string to pendulum DateTime.

    dlt's incremental compares cursor values; mixing string vs DateTime
    objects causes incorrect ordering.  This transform normalises the field
    to a ``pendulum.DateTime`` on every yielded row.
    """
    md = row.get("MetaData") or {}
    raw = md.get("LastUpdatedTime")
    if not isinstance(raw, str):
        return row
    parsed = pendulum.parse(raw)
    return {
        **row,
        "MetaData": {**md, "LastUpdatedTime": parsed},
    }
