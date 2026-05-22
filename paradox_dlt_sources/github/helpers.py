"""GitHub source helpers — auth classes and REST client factory.

Two auth implementations:

- ``GitHubAppAuth``: signs every request with a short-lived GitHub App
  installation token.  Mints a JWT (RS256, signed with the App's RSA private
  key), exchanges it at ``/app/installations/<id>/access_tokens`` for an
  installation access token (~1 h), caches it for ~50 min, then auto-refreshes.

- ``GitHubPATAuth``: attaches a static Personal Access Token as a Bearer
  header.  Simpler to set up; tied to one GitHub user account.

Both implement ``requests.auth.AuthBase`` so they drop into any
``requests``-based HTTP stack (including dlt's ``RESTClient``).
"""

from __future__ import annotations

import time

import jwt
import requests
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from requests import PreparedRequest
from requests.auth import AuthBase

from .settings import (
    GITHUB_API_BASE_URL,
    GITHUB_APP_JWT_TTL_SECONDS,
    GITHUB_INSTALLATION_TOKEN_TTL_SECONDS,
)

_HTTP_FORBIDDEN = 403


class GitHubAppAuth(AuthBase):
    """requests ``Auth`` that signs every call with a GitHub App installation token.

    Mints a JWT from ``(app_id, private_key)`` → exchanges it at
    ``/app/installations/<id>/access_tokens`` for a short-lived installation
    token → caches that and attaches it as ``Authorization: Bearer …`` on
    outgoing requests.  Refreshes automatically when the cached token nears
    expiry (after ~50 minutes out of the ~60-minute lifetime GitHub grants).

    Args:
        app_id: GitHub App ID (numeric string, e.g. ``"12345"``).
        installation_id: GitHub App installation ID for the target org.
        private_key: PEM-encoded RSA private key for the App (PKCS#8 or
            PKCS#1 format as downloaded from the GitHub App settings page).
    """

    def __init__(self, app_id: str, installation_id: str, private_key: str) -> None:
        self._app_id = str(app_id).strip()
        self._installation_id = str(installation_id).strip()
        self._private_key = private_key
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    def _mint_jwt(self) -> str:
        """Return a signed RS256 JWT valid for ``GITHUB_APP_JWT_TTL_SECONDS``."""
        now = int(time.time())
        return str(
            jwt.encode(
                {
                    "iat": now - 60,
                    "exp": now + GITHUB_APP_JWT_TTL_SECONDS,
                    "iss": self._app_id,
                },
                self._private_key,
                algorithm="RS256",
            )
        )

    def _refresh_installation_token(self) -> None:
        """Exchange a freshly-minted JWT for an installation access token."""
        url = f"{GITHUB_API_BASE_URL}/app/installations/{self._installation_id}/access_tokens"
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {self._mint_jwt()}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30,
        )
        resp.raise_for_status()
        self._token = resp.json()["token"]
        self._token_expires_at = time.time() + GITHUB_INSTALLATION_TOKEN_TTL_SECONDS

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self._token is None or time.time() >= self._token_expires_at:
            self._refresh_installation_token()
        request.headers["Authorization"] = f"Bearer {self._token}"
        return request


class GitHubPATAuth(AuthBase):
    """requests ``Auth`` that attaches a static Personal Access Token as Bearer.

    ``Authorization: Bearer <token>`` works for both classic and fine-grained
    PATs per GitHub's documentation (the older ``token <token>`` scheme is
    also accepted but Bearer is the documented preference).

    Args:
        pat_token: A GitHub Personal Access Token (classic or fine-grained).
    """

    def __init__(self, pat_token: str) -> None:
        self._pat_token = pat_token.strip()

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self._pat_token}"
        return request


def _raise_on_http_error(response: requests.Response, *args: object, **kwargs: object) -> None:
    """Session-level response hook that turns 4xx/5xx into ``HTTPError``.

    Without this, dlt's ``RESTClient.get(...)`` returns the response object
    even on error statuses — the caller then does ``.json()`` on the body
    and silently yields the error payload as a data row. The
    ``try/except HTTPError`` blocks scattered through this source assume
    auto-raise behavior; this hook makes that assumption true.

    (``RESTClient.paginate(...)`` already installs its own ``raise_for_status``
    handler internally, so this hook is redundant on the paginated path but
    not double-raising — ``raise_for_status`` is idempotent.)
    """
    response.raise_for_status()


def make_client(auth: AuthBase) -> RESTClient:
    """Return a ``RESTClient`` pre-configured for the GitHub REST API.

    Uses ``HeaderLinkPaginator`` (RFC 5988 ``Link: <url>; rel="next"``),
    which is GitHub's standard pagination mechanism for all list endpoints.

    Args:
        auth: An ``AuthBase`` instance — either ``GitHubAppAuth`` or
            ``GitHubPATAuth``.
    """
    session = requests.Session()
    session.hooks["response"].append(_raise_on_http_error)
    return RESTClient(
        base_url=GITHUB_API_BASE_URL,
        auth=auth,
        paginator=HeaderLinkPaginator(),
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        session=session,
    )


__all__ = [
    "GitHubAppAuth",
    "GitHubPATAuth",
    "make_client",
]
