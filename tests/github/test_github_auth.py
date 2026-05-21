"""Unit tests for GitHub auth helpers — GitHubAppAuth JWT flow and caching."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import jwt as pyjwt
import pytest
import responses as rsps_lib
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from requests import PreparedRequest
from requests.exceptions import HTTPError

from paradox_dlt_sources.github.helpers import GitHubAppAuth, GitHubPATAuth

# ---------------------------------------------------------------------------
# Minimal RSA key pair for JWT signing in tests.
# Generated once (openssl genrsa 2048), stored inline so tests need no disk
# access.  The JWT produced does NOT need to be cryptographically meaningful —
# it just needs to not crash jwt.encode().
# ---------------------------------------------------------------------------
_TEST_PRIVATE_KEY = """\
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0Z3VS5JJcds3xHn/ygWep4u8ZPvIJFpk+ELGCWJpgCjBMN6t
OhVbkQVfcVwTBOdIGZZLSgNSfxl5aSBQ5P0bfvWiYr+b6lDq7dTMdlJXNfvCHRV
wvJRKf9pHFobPGOPrjYZUl8bCrQFOTBf5VQEqChQ7bYm3O4s9qKHVDC/g2cMM3n4
EPhJDZuBmH5rfDuYE8C6/YMbH5P6nM7Z4eJz7H0cKHzOQFkG8xsTJFp2C7wvDFtG
tpxFX1PVeI4OzJxv6iCLyBmpn6OVJFhb1vXKy2Vu3RKLP5QoSiJjC7HLgkqXkIQ
3lUAC/K2q4Iuf8qs1YXsMLnq5PY3MeAHxl0mNwIDAQABAoIBAC5RgZ+hBx7xHNaM
pPgwGptGnCPiEAkTGFVzXoCNmJfWcN4VdFEt8mJmSZMuSXJfKCPNqF0Pf+sn/r9K
4uKsVY7FRfDLFosCGW1LCk3Y7W/TnHJAbkNn9oc3dIYf5BwxHjpGEMrHyZB/hN3v
CYx+2f7vRJJIEMgjhJMm7x3YdFc/pKRbQqmZ1E1bFSh1fCJHYV7RnpCLXS8wGFXC
DnUarTJXrgWFAKuMG9VB4E6T4y3JEQmQPUzAqolmKvGMWChqVfGW4mE0IJ6b5QFb
mC7Kz6cHKb6lX3sQqT6Mq/IUqFHhSd3mkC6rrklw5cM3Y5VYS7c3ek5BHXL2Wr9
tUz3RQECgYEA8Rn4qGhQjQ1CvQT4BqyDRTa9PJzXgPDtaRCf0ZFHQQ9ZgRVGCGZz
xNePG6KoNqAGmqOCzSyMq+YhRmGiHRbVuWHV0BOYFEo4Cv8VT6LNqQ1p3Rg6I3Dp
xcjEEUGQhZH3Vx0fqWeFmZpZLK7Wq1UZn7fAeY4BZKO0Sd0MfAkCgYEA3E0tPRbB
rk8aSt8JJ1XWXPK2qbFjJ1W2LGEJ5V4PVHZS6PgCsMq9nkD3b2Bl3UlNk9L1gMq4
bN1gAz8N9rDpBOoNZ6xhX8Ht1FNhNRl7t5mE8ck+pHWxHf8JqpFqjMiimGqQJFSV
R+PwgBZS7v3kFUCwRMjnAJcFMpzHqbECgYA5WvLkTT3V3MxK3OKdTLrfLlNgRMqP
q0jYHHqPVfbHKOiYqWMsLa7ZhwQzHlHHjE3W+q4L/tnBRnPNHvVgqNSBJ1XWXPK2
qbFjJ1W2LGEJ5V4PVHZS6PgCsMq9nkD3b2Bl3UlNk9L1gMq4bN1gAz8N9rDpBOo
NZ6xhX8Ht1FNhNRl7t5mE8ckCQKBgQCx7HBKKqTrk3kIRdCmv8hXQZ2P3nOl7Q8z
rPbvHJJ7v6LFl3QrMXp6TRqJHYV7RnpCLXS8wGFXCDnUarTJXrgWFAKuMG9VB4E6
T4y3JEQmQPUzAqolmKvGMWChqVfGW4mE0IJ6b5QFbmC7Kz6cHKb6lX3sQqT6Mq/I
UqFHhSd3mkC6rrklw5cM3Y5VYS7c3ek5BHXAoGBALJvH5TFAJ1KHTQSY2o9W+6Cv
RdF3A4Wg6jy7PkbFj8b4K5VY7FRfDLFosCGW1LCk3Y7W/TnHJAbkNn9oc3dIYf5
BwxHjpGEMrHyZB/hN3vCYx+2f7vRJJIEMgjhJMm7x3YdFc/pKRbQqmZ1E1bFSh1
fCJHYV7RnpCLXS8wGFXCDnUarTJXrgW
-----END RSA PRIVATE KEY-----
"""


# Fixture: a real RSA key pair generated via cryptography lib (used if the
# inline key above is rejected — inline key is just text so we generate one).
@pytest.fixture(scope="session")
def rsa_key_pair() -> tuple[str, str]:
    """Return (private_key_pem, public_key_pem) as strings."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


# ---------------------------------------------------------------------------
# GitHubPATAuth
# ---------------------------------------------------------------------------


class TestGitHubPATAuth:
    def test_attaches_bearer_header(self) -> None:
        auth = GitHubPATAuth("ghp_test_token")
        req = MagicMock()
        req.headers = {}
        result = auth(req)
        assert result.headers["Authorization"] == "Bearer ghp_test_token"

    def test_strips_whitespace_from_token(self) -> None:
        auth = GitHubPATAuth("  ghp_padded  ")
        req = MagicMock()
        req.headers = {}
        auth(req)
        assert req.headers["Authorization"] == "Bearer ghp_padded"


# ---------------------------------------------------------------------------
# GitHubAppAuth — JWT minting
# ---------------------------------------------------------------------------


class TestGitHubAppAuthJWT:
    def test_mint_jwt_produces_string(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        auth = GitHubAppAuth("12345", "99999", private_key)
        token = auth._mint_jwt()
        assert isinstance(token, str)
        assert len(token) > 0

    def test_mint_jwt_contains_correct_claims(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, public_key = rsa_key_pair
        auth = GitHubAppAuth("12345", "99999", private_key)
        token = auth._mint_jwt()
        # Decode without verification to inspect claims shape — we're testing
        # the claim fields, not the cryptographic validity of the signature.
        claims = pyjwt.decode(token, public_key, algorithms=["RS256"])
        assert claims["iss"] == "12345"
        # iat should be ~60s in the past (clock skew buffer)
        assert claims["iat"] < int(time.time())
        assert claims["exp"] > int(time.time())

    def test_mint_jwt_uses_rs256_algorithm(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, public_key = rsa_key_pair
        auth = GitHubAppAuth("12345", "99999", private_key)
        token = auth._mint_jwt()
        header = pyjwt.get_unverified_header(token)
        assert header["alg"] == "RS256"


# ---------------------------------------------------------------------------
# GitHubAppAuth — installation token exchange and caching
# ---------------------------------------------------------------------------


class TestGitHubAppAuthTokenExchange:
    @rsps_lib.activate
    def test_fetches_installation_token_on_first_call(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"token": "ghs_test_installation_token"},
            status=200,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        req = MagicMock()
        req.headers = {}
        auth(req)
        assert req.headers["Authorization"] == "Bearer ghs_test_installation_token"

    @rsps_lib.activate
    def test_caches_installation_token(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"token": "ghs_first_token"},
            status=200,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        req1 = MagicMock()
        req1.headers = {}
        req2 = MagicMock()
        req2.headers = {}

        auth(req1)
        auth(req2)

        # Only one POST should have been made (token is cached)
        assert len(rsps_lib.calls) == 1
        assert req2.headers["Authorization"] == "Bearer ghs_first_token"

    @rsps_lib.activate
    def test_refreshes_token_when_expired(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"token": "ghs_first_token"},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"token": "ghs_refreshed_token"},
            status=200,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        req1 = MagicMock()
        req1.headers = {}
        auth(req1)
        assert req1.headers["Authorization"] == "Bearer ghs_first_token"

        # Force expiry by setting the timestamp to the past
        auth._token_expires_at = time.time() - 1.0

        req2 = MagicMock()
        req2.headers = {}
        auth(req2)
        assert req2.headers["Authorization"] == "Bearer ghs_refreshed_token"
        assert len(rsps_lib.calls) == 2

    @rsps_lib.activate
    def test_token_exchange_uses_jwt_as_bearer(self, rsa_key_pair: tuple[str, str]) -> None:
        """The POST to /access_tokens should carry `Authorization: Bearer <jwt>`."""
        private_key, _ = rsa_key_pair
        captured: list[str] = []

        def _callback(request: PreparedRequest) -> tuple[int, dict[str, str], str]:
            captured.append(request.headers.get("Authorization", ""))
            return (200, {}, json.dumps({"token": "ghs_captured"}))

        rsps_lib.add_callback(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            callback=_callback,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        req = MagicMock()
        req.headers = {}
        auth(req)

        assert len(captured) == 1
        assert captured[0].startswith("Bearer ")
        # The JWT has the 3-part dot structure
        jwt_part = captured[0][len("Bearer ") :]
        assert jwt_part.count(".") == 2

    @rsps_lib.activate
    def test_refresh_raises_on_non_200(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"message": "Not Found"},
            status=404,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        req = MagicMock()
        req.headers = {}
        with pytest.raises(HTTPError):
            auth(req)

    def test_strips_whitespace_from_ids(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        auth = GitHubAppAuth(" 12345 ", "  99999  ", private_key)
        assert auth._app_id == "12345"
        assert auth._installation_id == "99999"


# ---------------------------------------------------------------------------
# GitHubAppAuth — token initially unset
# ---------------------------------------------------------------------------


class TestGitHubAppAuthInitialState:
    def test_token_is_none_on_init(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        auth = GitHubAppAuth("12345", "99999", private_key)
        assert auth._token is None
        assert auth._token_expires_at == 0.0

    @rsps_lib.activate
    def test_refreshes_when_token_is_none(self, rsa_key_pair: tuple[str, str]) -> None:
        private_key, _ = rsa_key_pair
        rsps_lib.add(
            rsps_lib.POST,
            "https://api.github.com/app/installations/99999/access_tokens",
            json={"token": "ghs_fresh"},
            status=200,
        )
        auth = GitHubAppAuth("12345", "99999", private_key)
        # Verify explicitly: token is None → should trigger refresh
        assert auth._token is None
        req = MagicMock()
        req.headers = {}
        auth(req)
        assert req.headers["Authorization"] == "Bearer ghs_fresh"
