"""iNova (UKG Ready) source helpers — OAuth auth, throttled GET, row shaping."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from typing import Any

import requests
from requests import PreparedRequest
from requests.auth import AuthBase

from .settings import (
    BASE_URL_TEMPLATE,
    ENDPOINT_TOKEN,
    PII_EXCLUDED_KEYS,
    REQUEST_TIMEOUT_SECONDS,
    SERVICE_LIMIT_STATUS,
    THROTTLE_DEFAULT_WAIT_SECONDS,
    THROTTLE_MAX_RETRIES,
    THROTTLE_STATUS,
    THROTTLE_WAIT_HEADER,
    TOKEN_REFRESH_SAFETY_SECONDS,
)

Row = dict[str, Any]


def build_base_url(pod: str, cid: str) -> str:
    """Return the v2 company base URL for a UKG Ready pod and company id.

    Args:
        pod: Tenant host, e.g. ``secure.saashr.com`` or an iNova white-label host.
        cid: Company id, unique id, or ``|short_name`` / ``!short_name``.
    """
    host = pod.strip().removeprefix("https://").removeprefix("http://").rstrip("/")
    return BASE_URL_TEMPLATE.format(pod=host, cid=cid.strip())


class ClientCredentialsAuth(AuthBase):
    """OAuth 2.0 ``client_credentials`` auth against the UKG Ready token endpoint.

    Fetches ``POST {base_url}/oauth2/token`` lazily and caches the token until
    shortly before ``expires_in``.

    Args:
        base_url: Company base URL (``.../ta/rest/v2/companies/{cid}``).
        client_id: OAuth client id of the Machine-to-Machine app.
        client_secret: OAuth client secret.
    """

    def __init__(self, *, base_url: str, client_id: str, client_secret: str) -> None:
        self._token_url = f"{base_url.rstrip('/')}{ENDPOINT_TOKEN}"
        self._client_id = client_id.strip()
        self._client_secret = client_secret
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if (
            self._access_token is None
            or time.time() >= self._expires_at - TOKEN_REFRESH_SAFETY_SECONDS
        ):
            self._refresh_access_token()
        request.headers["Authorization"] = f"Bearer {self._access_token}"
        return request

    def _refresh_access_token(self) -> None:
        response = requests.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            headers={"Accept": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = body["access_token"]
        self._expires_at = time.time() + float(body.get("expires_in", 3600))


class UkgReadyClient:
    """Minimal GET client honouring UKG Ready's 503 call-limit throttling.

    Error messages carry only status code and path — never response bodies,
    which may contain employee records.

    Args:
        base_url: Company base URL (``.../ta/rest/v2/companies/{cid}``).
        auth: ``requests`` auth, normally ``ClientCredentialsAuth``.
        sleep: Injectable sleep for tests.
    """

    def __init__(
        self,
        base_url: str,
        auth: AuthBase,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.auth = auth
        self._session.headers.update({"Accept": "application/json"})
        self._sleep = sleep

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET ``path`` and return the decoded JSON body.

        Args:
            path: Path relative to the company base URL.
            params: Optional query parameters.

        Raises:
            UkgReadyError: On 413 service limit, exhausted throttle retries, or
                any other non-2xx response.
        """
        url = f"{self.base_url}{path}"
        for _ in range(THROTTLE_MAX_RETRIES + 1):
            response = self._session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == THROTTLE_STATUS:
                self._sleep(throttle_wait_seconds(response))
                continue
            if response.status_code == SERVICE_LIMIT_STATUS:
                raise UkgReadyError(
                    f"UKG Ready service limit (413) on {path}; narrow the request window"
                )
            if not response.ok:
                raise UkgReadyError(f"UKG Ready GET {path} failed with HTTP {response.status_code}")
            return response.json() if response.content else {}
        raise UkgReadyError(f"UKG Ready GET {path} still throttled after retries")


class UkgReadyError(RuntimeError):
    """Raised for non-retryable UKG Ready API errors."""


def throttle_wait_seconds(response: requests.Response) -> float:
    """Seconds to wait after a 503, from ``X-CallLimit-TimeToWait`` (milliseconds).

    Args:
        response: The throttled response.
    """
    raw = response.headers.get(THROTTLE_WAIT_HEADER)
    try:
        return max(float(raw) / 1000.0, 0.0) if raw is not None else THROTTLE_DEFAULT_WAIT_SECONDS
    except ValueError:
        return THROTTLE_DEFAULT_WAIT_SECONDS


def strip_pii(value: Any) -> Any:
    """Recursively drop keys listed in ``PII_EXCLUDED_KEYS``, plus ``_links``.

    Args:
        value: A decoded JSON value.
    """
    if isinstance(value, dict):
        return {
            k: strip_pii(v)
            for k, v in value.items()
            if k.lower() not in PII_EXCLUDED_KEYS and k != "_links"
        }
    if isinstance(value, list):
        return [strip_pii(v) for v in value]
    return value


def date_windows(start: date, end: date, max_days: int) -> Iterator[tuple[date, date]]:
    """Split ``[start, end]`` into inclusive windows of at most ``max_days`` days.

    Args:
        start: First date (inclusive).
        end: Last date (inclusive).
        max_days: Maximum window length in days.
    """
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=max_days - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def nested_id(row: Row, key: str) -> Any:
    """Return ``row[key]["id"]`` when present, else ``None``.

    Args:
        row: Source row.
        key: Name of the nested object.
    """
    nested = row.get(key)
    return nested.get("id") if isinstance(nested, dict) else None
