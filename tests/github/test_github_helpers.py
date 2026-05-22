"""Unit tests for GitHub helpers — make_client factory and auth dispatch."""

from __future__ import annotations

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError

from paradox_dlt_sources.github import github_source
from paradox_dlt_sources.github.helpers import (
    GitHubPATAuth,
    make_client,
)
from paradox_dlt_sources.github.settings import GITHUB_API_BASE_URL


def _gen_private_key_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()


# ---------------------------------------------------------------------------
# make_client
# ---------------------------------------------------------------------------


class TestMakeClient:
    def test_returns_rest_client_with_pat_auth(self) -> None:
        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        assert isinstance(client, RESTClient)

    def test_base_url_matches_settings(self) -> None:
        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        assert client.base_url == GITHUB_API_BASE_URL

    def test_client_uses_header_link_paginator(self) -> None:
        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        assert isinstance(client.paginator, HeaderLinkPaginator)

    def test_client_headers_include_github_accept(self) -> None:
        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        headers = client.headers or {}
        assert headers.get("Accept") == "application/vnd.github+json"

    def test_client_headers_include_api_version(self) -> None:
        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        headers = client.headers or {}
        assert headers.get("X-GitHub-Api-Version") == "2022-11-28"

    def test_get_raises_http_error_on_4xx_response(self) -> None:
        # Regression: prior to the response-hook fix, dlt RESTClient.get() did
        # not raise on 4xx — error bodies (e.g. GitHub rate-limit JSON with
        # `message`/`documentation_url`/`status` fields) silently got yielded
        # as data rows by every `client.get(...).json()` callsite in the
        # github resource. The session-level hook installed by `make_client`
        # forces auto-raise behavior so the existing `try/except HTTPError`
        # blocks work as designed.
        rate_limit_body = (
            b'{"message":"API rate limit exceeded for user ID 1839452",'
            b'"documentation_url":"https://docs.github.com/en/rest/.../rate-limiting",'
            b'"status":"403"}'
        )

        class _RateLimitAdapter(HTTPAdapter):
            def send(self, request, **kwargs):  # type: ignore[override]
                r = requests.Response()
                r.status_code = 403
                r._content = rate_limit_body
                r.headers["Content-Type"] = "application/json"
                r.request = request
                return r

        auth = GitHubPATAuth("ghp_test")
        client = make_client(auth)
        client.session.mount("https://", _RateLimitAdapter())

        with pytest.raises(HTTPError) as exc_info:
            client.get("/orgs/anything")
        assert exc_info.value.response is not None
        assert exc_info.value.response.status_code == 403


# ---------------------------------------------------------------------------
# github_source auth dispatch
# ---------------------------------------------------------------------------


class TestGithubSourceAuthDispatch:
    def test_all_none_builds_placeholder_without_error(self) -> None:
        """Decorator-time call with no credentials should not raise."""
        src = github_source(org_logins=["fake-org"])
        assert src is not None

    def test_pat_only_builds_source(self) -> None:
        src = github_source(org_logins=["fake-org"], pat_token="ghp_test")
        assert src is not None

    def test_full_app_creds_builds_source(self) -> None:
        private_pem = _gen_private_key_pem()
        src = github_source(
            org_logins=["fake-org"],
            app_id="12345",
            installation_id="99999",
            private_key=private_pem,
        )
        assert src is not None

    def test_pat_plus_app_id_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="both PAT and App"):
            github_source(
                org_logins=["fake-org"],
                pat_token="ghp_test",
                app_id="12345",
            )

    def test_partial_app_creds_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="app_id"):
            github_source(
                org_logins=["fake-org"],
                app_id="12345",
                # installation_id and private_key intentionally omitted
            )

    def test_only_installation_id_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="installation_id"):
            github_source(
                org_logins=["fake-org"],
                installation_id="99999",
            )
