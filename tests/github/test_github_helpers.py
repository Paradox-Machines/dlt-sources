"""Unit tests for GitHub helpers — make_client factory and auth dispatch."""

from __future__ import annotations

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

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
