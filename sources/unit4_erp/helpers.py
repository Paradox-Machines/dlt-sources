"""Unit4 ERP source — auth, throttling, keyset paging, filters and row transforms."""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import threading
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

import dlt
import requests
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.sources.helpers.requests import Client
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from requests import PreparedRequest, Request, Response
from requests.auth import AuthBase, HTTPBasicAuth

from .settings import (
    AUTH_TYPES,
    COMPANY_KEY,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_SCOPE,
    DEFAULT_TOKEN_TTL_SECONDS,
    MAX_RECORDS_PER_CALL,
    MAX_RETRY_DELAY_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    SOURCE_ACCOUNT_COLUMN,
    STANDARD_COLUMNS,
    TOKEN_REFRESH_SAFETY_SECONDS,
    UPDATED_AT_PATH,
    USER_AGENT,
    Key,
    UnitObject,
)

LOGGER = logging.getLogger(__name__)

Row = dict[str, Any]

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class ClientCredentialsAuth(AuthBase):
    """OAuth 2.0 client-credentials bearer auth against Unit4 IDS.

    The token is fetched lazily and cached until ``TOKEN_REFRESH_SAFETY_SECONDS``
    before its expiry; Unit4 tokens live one hour and a GL backfill outlives one.

    Args:
        token_url: The tenant's IDS token endpoint.
        client_id: M2M client id.
        client_secret: M2M client secret.
        scope: OAuth scope (``u4erp``).
    """

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._access_token: str | None = None
        self._expires_at = 0.0

    def __repr__(self) -> str:
        """Return a repr that never leaks the client secret."""
        return f"ClientCredentialsAuth(token_url={self._token_url!r})"

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Attach the bearer token, refreshing it if stale."""
        request.headers["Authorization"] = f"Bearer {self.access_token()}"
        return request

    def access_token(self) -> str:
        """Return a valid access token, fetching a new one when needed."""
        if (
            self._access_token is None
            or time.time() >= self._expires_at - TOKEN_REFRESH_SAFETY_SECONDS
        ):
            self._refresh_access_token()
        assert self._access_token is not None
        return self._access_token

    def _refresh_access_token(self) -> None:
        """Fetch and cache a fresh access token."""
        response = requests.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scope,
            },
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = str(body["access_token"])
        self._expires_at = time.time() + float(body.get("expires_in", DEFAULT_TOKEN_TTL_SECONDS))


class RequestThrottle:
    """Space requests so the tenant-wide per-minute limit is never reached.

    Unit4 counts every M2M client of a tenant against one 500/minute window, so
    other integrations on the tenant share it.

    Args:
        max_per_minute: Requests per minute; ``0`` disables throttling.
    """

    def __init__(self, max_per_minute: int) -> None:
        self._interval = 60.0 / max_per_minute if max_per_minute > 0 else 0.0
        self._next_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        """Block until the next request slot."""
        if not self._interval:
            return
        with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            self._next_at = max(now, self._next_at) + self._interval
        if delay > 0:
            time.sleep(delay)


class ThrottledAuth(AuthBase):
    """Wrap an auth so every request (retries included) waits for a throttle slot.

    Args:
        inner: The real auth.
        throttle: Shared request throttle.
    """

    def __init__(self, inner: AuthBase, throttle: RequestThrottle) -> None:
        self._inner = inner
        self._throttle = throttle

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Wait for a slot, then apply the wrapped auth."""
        self._throttle.wait()
        request.headers["User-Agent"] = USER_AGENT
        result: PreparedRequest = self._inner(request)
        return result


def make_auth(credentials: Mapping[str, Any]) -> AuthBase:
    """Build the auth named by ``credentials["type"]``.

    Which one RMU's ERP CR tenant accepts is still open, so both are supported:

    * ``oauth_client_credentials``: ``token_url``, ``client_id``, ``client_secret``,
      optional ``scope`` (default ``u4erp``).
    * ``basic``: ``username``, ``password`` (an ERP user).

    Args:
        credentials: Credentials mapping from secrets.

    Raises:
        ValueError: On an unknown type or a missing key.
    """
    kind = credentials.get("type")
    if kind not in AUTH_TYPES:
        raise ValueError(f"unit4_erp: credentials.type must be one of {AUTH_TYPES}, got {kind!r}")
    required = (
        ("token_url", "client_id", "client_secret")
        if kind == "oauth_client_credentials"
        else ("username", "password")
    )
    missing = [k for k in required if not credentials.get(k)]
    if missing:
        raise ValueError(f"unit4_erp: credentials ({kind}) missing {', '.join(missing)}")
    if kind == "basic":
        return HTTPBasicAuth(str(credentials["username"]), str(credentials["password"]))
    return ClientCredentialsAuth(
        token_url=str(credentials["token_url"]),
        client_id=str(credentials["client_id"]),
        client_secret=str(credentials["client_secret"]),
        scope=str(credentials.get("scope") or DEFAULT_SCOPE),
    )


def make_client(
    auth: AuthBase,
    base_url: str,
    max_requests_per_minute: int,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> RESTClient:
    """Return a throttled ``RESTClient`` for the tenant's Object API.

    The session retries 429 and 5xx (Unit4 asks for backoff on 503, and a
    suspended non-production environment answers slowly while it wakes up); a
    429 waits for ``Retry-After``.

    Args:
        auth: Normally from ``make_auth``.
        base_url: Tenant and environment API root.
        max_requests_per_minute: Client-side request ceiling; ``0`` disables it.
        max_attempts: Attempts per request before giving up.
    """
    session = Client(
        request_timeout=REQUEST_TIMEOUT_SECONDS,
        raise_for_status=False,
        request_max_attempts=max_attempts,
        request_max_retry_delay=MAX_RETRY_DELAY_SECONDS,
        respect_retry_after_header=True,
    ).session
    return RESTClient(
        base_url=base_url.rstrip("/"),
        auth=ThrottledAuth(auth, RequestThrottle(max_requests_per_minute)),
        session=session,
        headers={"Accept": "application/json"},
    )


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def literal(value: Any, key: Key) -> str:
    """Render a key value as an Object API filter literal.

    Args:
        value: Value read from the last row of a page.
        key: Key definition (decides quoting).

    Raises:
        ValueError: If the value is missing: keyset paging cannot continue past it.
    """
    if value is None:
        raise ValueError(f"unit4_erp: key {key.path!r} is null; keyset paging cannot continue")
    if key.kind == "bigint":
        return str(int(value))
    if key.kind == "date":
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def timestamp_literal(value: dt.datetime, tz: ZoneInfo) -> str:
    """Render a timestamp literal in the declared source timezone."""
    local = value.astimezone(tz)
    if local.utcoffset() == dt.timedelta(0):
        return local.strftime("%Y-%m-%dT%H:%M:%SZ")
    return local.isoformat(timespec="seconds")


def keyset_filter(keys: Sequence[Key], values: Sequence[Any]) -> str:
    """Return the predicate selecting rows strictly after ``values`` in key order.

    ``(k1 gt v1) or (k1 eq v1 and k2 gt v2) or …``

    Args:
        keys: Ordered key fields.
        values: Key values of the last row read, in the same order.
    """
    clauses = []
    for i, key in enumerate(keys):
        parts = [f"{k.path} eq {literal(v, k)}" for k, v in zip(keys[:i], values[:i], strict=True)]
        parts.append(f"{key.path} gt {literal(values[i], key)}")
        clauses.append("(" + " and ".join(parts) + ")")
    return " or ".join(clauses)


def and_filters(*filters: str | None) -> str | None:
    """AND together the non-empty filters, parenthesised."""
    present = [f for f in filters if f]
    if not present:
        return None
    if len(present) == 1:
        return present[0]
    return " and ".join(f"({f})" for f in present)


def key_select(keys: Sequence[Key]) -> str:
    """Build a ``select`` that returns only the key fields.

    Nested fields use Unit4's ``parent(child)`` form: ``a/b`` → ``a(b)``.
    """
    top: list[str] = []
    nested: dict[str, list[str]] = {}
    for key in keys:
        if "/" in key.path:
            parent, child = key.path.split("/", 1)
            if parent not in nested:
                top.append(parent)
                nested[parent] = []
            nested[parent].append(child)
        else:
            top.append(key.path)
    return ",".join(f"{t}({','.join(nested[t])})" if t in nested else t for t in top)


def paging_keys(obj: UnitObject) -> tuple[Key, ...]:
    """Keys to order and page on: ``companyId`` is fixed per request, so drop it."""
    rest = tuple(k for k in obj.keys if k.path != COMPANY_KEY)
    return rest or obj.keys


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def get_path(row: Mapping[str, Any], path: str) -> Any:
    """Read ``a/b`` from a nested record; ``None`` when any level is missing."""
    value: Any = row
    for part in path.split("/"):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


class KeysetPaginator(BasePaginator):
    """Keyset paginator for the Object API.

    Orders on the key fields and, after each page, filters past the last key
    read.  A page shorter than ``limit`` (empty included) ends the walk.

    Args:
        keys: Ordered key fields.
        limit: Rows per call (at most ``MAX_RECORDS_PER_CALL``).
        base_filter: Filter every page must also satisfy (the change cursor).
    """

    def __init__(self, keys: Sequence[Key], limit: int, base_filter: str | None = None) -> None:
        super().__init__()
        if not 0 < limit <= MAX_RECORDS_PER_CALL:
            raise ValueError(f"unit4_erp: page_size must be 1..{MAX_RECORDS_PER_CALL}")
        self.keys = tuple(keys)
        self.limit = limit
        self.base_filter = base_filter
        self._last: list[Any] | None = None

    def init_request(self, request: Request) -> None:
        """Seed ``limit``, ``orderBy`` and the base filter on the first request."""
        if request.params is None:
            request.params = {}
        request.params["limit"] = self.limit
        request.params["orderBy"] = ",".join(k.path for k in self.keys)
        if self.base_filter:
            request.params["filter"] = self.base_filter

    def update_state(self, response: Response, data: list[Any] | None = None) -> None:
        """Remember the last key; stop on a short page."""
        rows = data or []
        if len(rows) > self.limit:
            raise ValueError(
                f"unit4_erp: {response.url} returned {len(rows)} rows for limit={self.limit}; "
                "the server ignored the page size, so paging would be unreliable"
            )
        self._has_next_page = len(rows) == self.limit
        if rows:
            self._last = [get_path(rows[-1], k.path) for k in self.keys]

    def update_request(self, request: Request) -> None:
        """Filter past the last key read.

        dlt calls this after every page, the last one too, so it is a no-op
        once the walk is over.
        """
        if not self._has_next_page or self._last is None:
            return
        if request.params is None:
            request.params = {}
        request.params["filter"] = and_filters(
            self.base_filter, keyset_filter(self.keys, self._last)
        )


def iter_pages(
    client: RESTClient,
    obj: UnitObject,
    company_id: str,
    *,
    page_size: int,
    select: str,
    base_filter: str | None = None,
) -> Iterator[list[Row]]:
    """Walk one object for one company with keyset paging.

    Args:
        client: Object API client.
        obj: Object to read.
        company_id: Company (``companyId``) to read.
        page_size: Rows per call.
        select: ``select`` parameter.
        base_filter: Extra filter (the change cursor), ANDed with the keyset.
    """
    params: dict[str, Any] = {**obj.params, "companyId": company_id, "select": select}
    paginator = KeysetPaginator(paging_keys(obj), page_size, base_filter)
    for page in client.paginate(obj.path, params=params, paginator=paginator):
        yield list(page)


# ---------------------------------------------------------------------------
# Row transforms and schema hints
# ---------------------------------------------------------------------------

_FRACTION = re.compile(r"(\.\d{6})\d+")


def parse_timestamp(value: Any, tz: ZoneInfo) -> dt.datetime | None:
    """Parse a Unit4 timestamp to aware UTC.

    .NET writes up to 7 fractional digits; Python keeps 6.  A value without an
    offset is read in the declared source timezone (CONNECTOR_SPEC §8.2).
    """
    if not value:
        return None
    text = _FRACTION.sub(r"\1", str(value)).replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(dt.UTC)


def transform_row(
    row: Mapping[str, Any],
    obj: UnitObject,
    company_id: str,
    tz: ZoneInfo,
    extracted_at: dt.datetime,
) -> Row:
    """Add standard columns and lift nested key fields to top-level columns.

    Nested objects are loaded as ``json`` (``max_table_nesting=0``), so a key
    such as ``additionalInformation/sequenceNumber`` is copied to
    ``additionalInformation__sequenceNumber`` for the primary key.

    Args:
        row: Raw Object API record.
        obj: Object it came from.
        company_id: Company it was read with.
        tz: Declared source timezone.
        extracted_at: Run timestamp.
    """
    out: Row = dict(row)
    for key in obj.keys:
        if "/" in key.path:
            out[key.column] = get_path(row, key.path)
    out[SOURCE_ACCOUNT_COLUMN] = company_id
    out["_source_created_at"] = None
    out["_source_updated_at"] = parse_timestamp(get_path(row, UPDATED_AT_PATH), tz)
    out["_is_deleted"] = False
    out["_deleted_at"] = None
    out["_extracted_at"] = extracted_at
    return out


def primary_key(obj: UnitObject) -> list[str]:
    """``_source_account_id`` plus the object's key columns (``companyId`` excluded).

    ``companyId`` equals ``_source_account_id`` on every row, so keeping both
    would only duplicate it.
    """
    return [SOURCE_ACCOUNT_COLUMN, *(k.column for k in obj.keys if k.path != COMPANY_KEY)]


def columns(obj: UnitObject) -> dict[str, dict[str, Any]]:
    """Column hints: standard columns plus key columns, fixed from the first load.

    Each column gets its own dict; dlt mutates hints in place.
    """
    hints: dict[str, dict[str, Any]] = {
        name: {"data_type": dtype, "nullable": name not in ("_is_deleted",)}
        for name, dtype in STANDARD_COLUMNS.items()
    }
    for key in obj.keys:
        dtype = "bigint" if key.kind == "bigint" else "text"
        hints[key.column] = {"data_type": dtype, "nullable": False}
    return hints


# ---------------------------------------------------------------------------
# Delete detection (CONNECTOR_SPEC §12.2 method 4: key reconciliation)
# ---------------------------------------------------------------------------

KeyTuple = tuple[str, ...]

_DLT_COLUMNS = ("_dlt_load_id", "_dlt_id")


def key_tuple(row: Mapping[str, Any], names: Sequence[str]) -> KeyTuple:
    """Comparable key of a row: values as text, so ``5`` and ``"5"`` match."""
    return tuple("" if row.get(n) is None else str(row.get(n)) for n in names)


def read_stored_rows(table: str, account_id: str) -> list[Row]:
    """Return the live (not deleted) rows of one company already in the destination.

    Read through the pipeline's SQL client, so it works on any SQL-queryable
    destination (duckdb, Iceberg via the filesystem SQL client).  Returns ``[]``
    when the table does not exist yet.

    Args:
        table: Destination table name.
        account_id: ``_source_account_id`` to keep.
    """
    pipeline = dlt.current.pipeline()
    schema = dlt.current.source_schema()
    if table not in schema.tables:
        return []
    json_columns = {
        name
        for name, col in schema.get_table_columns(table, include_incomplete=True).items()
        if col.get("data_type") == "json"
    }
    try:
        with pipeline.sql_client() as client:
            qualified = client.make_qualified_table_name(table)
            with client.execute_query(f"SELECT * FROM {qualified}") as cursor:
                names = [d[0] for d in cursor.description]
                fetched = cursor.fetchall()
    except (DatabaseUndefinedRelation, DestinationUndefinedEntity):
        return []
    rows = []
    for values in fetched:
        row = dict(zip(names, values, strict=True))
        if row.get(SOURCE_ACCOUNT_COLUMN) != account_id or row.get("_is_deleted"):
            continue
        for name in _DLT_COLUMNS:
            row.pop(name, None)
        for name in json_columns:
            if isinstance(row.get(name), str):
                row[name] = json.loads(row[name])
        rows.append(row)
    return rows


def tombstones(
    stored: Iterable[Row],
    stored_key_names: Sequence[str],
    live_keys: set[KeyTuple],
    deleted_at: dt.datetime,
) -> list[Row]:
    """Re-emit every stored row whose key is no longer live, marked deleted.

    The whole stored row is re-emitted (not only its key) because an upsert
    replaces the row: a key-only tombstone would wipe its data (§12.1 keeps it).

    Args:
        stored: Live rows already in the destination, normalised column names.
        stored_key_names: Normalised primary-key columns, in key order.
        live_keys: Keys the source still holds (or emitted this run).
        deleted_at: When the delete was detected.
    """
    out = []
    for row in stored:
        if key_tuple(row, stored_key_names) in live_keys:
            continue
        out.append(
            {**row, "_is_deleted": True, "_deleted_at": deleted_at, "_extracted_at": deleted_at}
        )
    return out


def normalized_names(names: Sequence[str]) -> list[str]:
    """Normalise column names with the source schema's naming convention."""
    naming = dlt.current.source_schema().naming
    return [naming.normalize_identifier(n) for n in names]


def check_reconciliation(stored: Sequence[Row], live_keys: set[KeyTuple], *, label: str) -> None:
    """Refuse to tombstone a whole company from an empty key read.

    An empty read against a populated table is far more likely a lost
    permission or a wrong ``companyId`` than every record being deleted.

    Raises:
        RuntimeError: When the source returned no keys but rows are loaded.
    """
    if stored and not live_keys:
        raise RuntimeError(
            f"unit4_erp: {label}: the source returned no keys but {len(stored)} rows are "
            "loaded; refusing to mark them all deleted. Check the M2M user's access."
        )
