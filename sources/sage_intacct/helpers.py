"""HTTP auth, query-service pagination and schema helpers for Sage Intacct."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

import requests
from dlt.common.data_types import TDataType
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns

from .settings import (
    CURSOR_FIELD,
    ENTITY_ID_COLUMN,
    MAX_PAGE_SIZE,
    PRIMARY_KEY,
    QUERY_PATH,
    TOKEN_EXPIRY_MARGIN_SECONDS,
    TOKEN_PATH,
    SageObject,
)

_NAMING = NamingConvention()

JsonDict = dict[str, Any]


class SageIntacctError(RuntimeError):
    """Raised when the Sage Intacct API returns an error response."""


def build_username(user_id: str, company_id: str, entity_id: str | None = None) -> str:
    """Build the OAuth ``username`` for a Web Services user.

    Args:
        user_id: Web Services user ID.
        company_id: Sage Intacct company ID.
        entity_id: Entity ID to scope the token to; falsy means top level.

    Returns:
        ``user@company`` or ``user@company|entity``.
    """
    username = f"{user_id}@{company_id}"
    return f"{username}|{entity_id}" if entity_id else username


def _error_message(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:500]
    error = body.get("ia::error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        return f"{error.get('code', '')}: {error.get('message', '')}".strip(": ")
    return str(body)[:500]


def _raise_for_status(response: requests.Response, context: str) -> None:
    if response.ok:
        return
    raise SageIntacctError(
        f"{context} failed with HTTP {response.status_code}: {_error_message(response)}"
    )


class SageIntacctAuth(requests.auth.AuthBase):
    """OAuth 2.0 client-credentials bearer auth with a cached, auto-refreshed token.

    The token is fetched lazily, reused until ``expires_in`` (minus
    ``TOKEN_EXPIRY_MARGIN_SECONDS``) has elapsed, and fetched again after that
    or after ``invalidate()``.

    Args:
        client_id: Client ID of the Sage developer-console application.
        client_secret: Client secret of that application.
        username: ``user@company`` or ``user@company|entity``.
        base_url: REST API base URL.
        session: Session used for token requests.
        clock: Monotonic clock in seconds; injectable for tests.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        username: str,
        base_url: str,
        session: requests.Session | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self._clock = clock
        self._token: str | None = None
        self._expires_at = 0.0

    def invalidate(self) -> None:
        """Drop the cached token so the next request fetches a new one."""
        self._token = None
        self._expires_at = 0.0

    def get_token(self) -> str:
        """Return a valid access token, fetching a new one if needed.

        Returns:
            The bearer access token.

        Raises:
            SageIntacctError: If the token endpoint rejects the request.
        """
        if self._token is not None and self._clock() < self._expires_at:
            return self._token
        response = self.session.post(
            f"{self.base_url}{TOKEN_PATH}",
            json={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
            },
            timeout=60,
        )
        _raise_for_status(response, f"Token request for {self.username}")
        body = response.json()
        token = body.get("access_token")
        if not token:
            raise SageIntacctError(f"Token response for {self.username} has no access_token")
        expires_in = float(body.get("expires_in") or 0)
        self._token = str(token)
        self._expires_at = self._clock() + max(expires_in - TOKEN_EXPIRY_MARGIN_SECONDS, 0.0)
        return self._token

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        """Attach the bearer token to ``request``."""
        request.headers["Authorization"] = f"Bearer {self.get_token()}"
        return request


def column_name(field: str) -> str:
    """Map a Sage dotted field path to its column name.

    Args:
        field: Field path such as ``audit.modifiedDateTime``.

    Returns:
        The dlt-normalised name, e.g. ``audit__modified_date_time``.
    """
    return "__".join(_NAMING.normalize_identifier(part) for part in field.split("."))


def flatten_record(record: Mapping[str, Any], prefix: str = "") -> JsonDict:
    """Flatten nested objects in a query result into dotted field paths.

    The query service may return requested sub-fields either as dotted keys
    (``"glAccount.id"``) or nested objects (``{"glAccount": {"id": ...}}``);
    both flatten to the same dotted path.

    Args:
        record: One ``ia::result`` item.
        prefix: Path prefix used for recursion.

    Returns:
        A flat dict keyed by dotted field path.
    """
    flat: JsonDict = {}
    for name, value in record.items():
        path = f"{prefix}{name}"
        if isinstance(value, Mapping):
            flat.update(flatten_record(value, f"{path}."))
        else:
            flat[path] = value
    return flat


def to_row(record: Mapping[str, Any], entity_id: str) -> JsonDict:
    """Convert a query result into a table row tagged with ``entity_id``.

    Args:
        record: One ``ia::result`` item.
        entity_id: Entity the row was read from.

    Returns:
        The row keyed by column name.
    """
    row = {column_name(path): value for path, value in flatten_record(record).items()}
    row[ENTITY_ID_COLUMN] = entity_id
    return row


def build_query(
    obj: SageObject,
    start: int,
    size: int,
    modified_since: str | None = None,
) -> JsonDict:
    """Build a query-service request body.

    Args:
        obj: Object to query.
        start: 1-based offset of the first record.
        size: Page size.
        modified_since: Inclusive lower bound on ``audit.modifiedDateTime``.

    Returns:
        The JSON body for ``POST /services/core/query``.
    """
    body: JsonDict = {
        "object": obj.object,
        "fields": list(obj.fields),
        "orderBy": [{"key": "asc"}],
        "start": start,
        "size": size,
    }
    if modified_since is not None:
        body["filters"] = [{"$gte": {CURSOR_FIELD: modified_since}}]
    return body


def query_pages(
    session: requests.Session,
    auth: SageIntacctAuth,
    obj: SageObject,
    page_size: int = MAX_PAGE_SIZE,
    modified_since: str | None = None,
) -> Iterator[list[JsonDict]]:
    """Yield pages of raw ``ia::result`` records for one object.

    Follows ``ia::meta.next`` until it is null.  A 401 drops the cached token
    and retries the page once with a fresh one.

    Args:
        session: HTTP session.
        auth: Auth for the entity scope being read.
        obj: Object to query.
        page_size: Records per request (1..``MAX_PAGE_SIZE``).
        modified_since: Inclusive lower bound on ``audit.modifiedDateTime``.

    Yields:
        Lists of records, one per page.

    Raises:
        ValueError: If ``page_size`` is out of range.
        SageIntacctError: On a non-2xx response.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}")
    url = f"{auth.base_url}{QUERY_PATH}"
    start: int | None = 1
    while start is not None:
        body = build_query(obj, start, page_size, modified_since)
        response = session.post(url, json=body, auth=auth, timeout=120)
        if response.status_code == requests.codes.unauthorized:
            auth.invalidate()
            response = session.post(url, json=body, auth=auth, timeout=120)
        _raise_for_status(response, f"Query {obj.object} (start={start})")
        payload = response.json()
        records = payload.get("ia::result") or []
        if records:
            yield records
        next_start = (payload.get("ia::meta") or {}).get("next")
        start = int(next_start) if next_start else None


def columns_for(obj: SageObject) -> TTableSchemaColumns:
    """Build column hints for an object, including the ``entity_id`` tag.

    Args:
        obj: Object to describe.

    Returns:
        Column hints keyed by column name; primary-key columns are non-null.
    """
    columns: TTableSchemaColumns = {}
    entity_column: TColumnSchema = {
        "name": ENTITY_ID_COLUMN,
        "data_type": "text",
        "nullable": False,
        "primary_key": True,
    }
    columns[ENTITY_ID_COLUMN] = entity_column
    for field, data_type in obj.fields.items():
        name = column_name(field)
        column: TColumnSchema = {
            "name": name,
            "data_type": cast(TDataType, data_type),
            "nullable": True,
        }
        if name in PRIMARY_KEY:
            column["nullable"] = False
            column["primary_key"] = True
        columns[name] = column
    return columns


def entity_scopes(entity_ids: Sequence[str] | None, top_level_id: str) -> list[str]:
    """Return the entity IDs to iterate; an empty list means top level only.

    Args:
        entity_ids: Configured entity IDs.
        top_level_id: Tag used for the top-level scope.

    Returns:
        De-duplicated, order-preserving entity IDs, or ``[top_level_id]``.
    """
    scopes = list(dict.fromkeys(e.strip() for e in entity_ids or () if e and e.strip()))
    return scopes or [top_level_id]
