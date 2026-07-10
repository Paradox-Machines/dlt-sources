"""Unit tests for m365_graph helpers — auth, classification, transforms."""

from __future__ import annotations

import base64
import time
from unittest.mock import MagicMock

import pytest
import responses as rsps_lib

from paradox_dlt_sources.m365_graph.helpers import (
    ClientCredentialsAuth,
    classify_mail_document,
    classify_sharepoint_document,
    conversation_depth,
    is_return_filename,
    is_teaser_filename,
    normalize_subject,
    round_from_filename,
    sender_domain,
    subject_is_reply,
)
from paradox_dlt_sources.m365_graph.settings import (
    ROLE_COUNTERPARTY_RETURN,
    ROLE_FRESH_NDA,
    ROLE_TEASER,
)

_LOGIN = "https://login.microsoftonline.com"


def _conv_index(depth: int) -> str:
    """Return a base64 conversationIndex encoding a message at the given depth."""
    return base64.b64encode(b"\x01" * (22 + 5 * depth)).decode()


# ---------------------------------------------------------------------------
# ClientCredentialsAuth
# ---------------------------------------------------------------------------


class TestClientCredentialsAuth:
    def _auth(self) -> ClientCredentialsAuth:
        return ClientCredentialsAuth(
            tenant_id="test-tenant",
            client_id="client-id",
            client_secret="secret",
            login_base_url=_LOGIN,
        )

    def test_token_url_is_tenant_scoped_v2(self) -> None:
        auth = self._auth()
        assert auth._token_url == (
            "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token"
        )

    @rsps_lib.activate
    def test_fetches_and_attaches_bearer_token(self) -> None:
        rsps_lib.add(
            rsps_lib.POST,
            f"{_LOGIN}/test-tenant/oauth2/v2.0/token",
            json={"access_token": "abc123", "expires_in": 3600},
            status=200,
        )
        auth = self._auth()
        req = MagicMock()
        req.headers = {}
        auth(req)
        assert req.headers["Authorization"] == "Bearer abc123"
        # Request body carries the client-credentials grant + default scope.
        body = rsps_lib.calls[0].request.body
        assert "grant_type=client_credentials" in body
        assert "scope=https%3A%2F%2Fgraph.microsoft.com%2F.default" in body

    @rsps_lib.activate
    def test_caches_token_across_calls(self) -> None:
        rsps_lib.add(
            rsps_lib.POST,
            f"{_LOGIN}/test-tenant/oauth2/v2.0/token",
            json={"access_token": "abc123", "expires_in": 3600},
            status=200,
        )
        auth = self._auth()
        for _ in range(3):
            req = MagicMock()
            req.headers = {}
            auth(req)
        # One network round-trip despite three signed requests.
        assert len(rsps_lib.calls) == 1

    @rsps_lib.activate
    def test_refreshes_when_token_near_expiry(self) -> None:
        rsps_lib.add(
            rsps_lib.POST,
            f"{_LOGIN}/test-tenant/oauth2/v2.0/token",
            json={"access_token": "first", "expires_in": 3600},
            status=200,
        )
        rsps_lib.add(
            rsps_lib.POST,
            f"{_LOGIN}/test-tenant/oauth2/v2.0/token",
            json={"access_token": "second", "expires_in": 3600},
            status=200,
        )
        auth = self._auth()
        req = MagicMock()
        req.headers = {}
        auth(req)
        # Force the cached token past the safety window.
        auth._expires_at = time.time() - 1
        auth(req)
        assert req.headers["Authorization"] == "Bearer second"
        assert len(rsps_lib.calls) == 2


# ---------------------------------------------------------------------------
# conversation_depth
# ---------------------------------------------------------------------------


class TestConversationDepth:
    def test_root_message_is_depth_zero(self) -> None:
        assert conversation_depth(_conv_index(0)) == 0

    @pytest.mark.parametrize("depth", [1, 2, 5])
    def test_reply_depth(self, depth: int) -> None:
        assert conversation_depth(_conv_index(depth)) == depth

    def test_none_and_empty_are_depth_zero(self) -> None:
        assert conversation_depth(None) == 0
        assert conversation_depth("") == 0

    def test_malformed_base64_is_depth_zero(self) -> None:
        assert conversation_depth("not valid base64 !!!") == 0

    def test_too_short_header_is_depth_zero(self) -> None:
        assert conversation_depth(base64.b64encode(b"\x01" * 10).decode()) == 0


# ---------------------------------------------------------------------------
# subject helpers
# ---------------------------------------------------------------------------


class TestSubjectHelpers:
    @pytest.mark.parametrize(
        "subject",
        ["RE: NDA", "re: nda", "FW: NDA", "Fwd: NDA", "AW: NDA", "RE[2]: NDA"],
    )
    def test_reply_prefixes_detected(self, subject: str) -> None:
        assert subject_is_reply(subject) is True

    @pytest.mark.parametrize("subject", ["Mutual NDA", "Project Falcon", "", None])
    def test_non_reply_subjects(self, subject: str | None) -> None:
        assert subject_is_reply(subject) is False

    def test_normalize_strips_nested_prefixes(self) -> None:
        assert normalize_subject("RE: FW:  Mutual NDA ") == "mutual nda"

    def test_normalize_none(self) -> None:
        assert normalize_subject(None) == ""


# ---------------------------------------------------------------------------
# filename classification
# ---------------------------------------------------------------------------


class TestFilenameHelpers:
    def test_teaser_detection(self) -> None:
        assert is_teaser_filename("Project Teaser.pdf") is True
        assert is_teaser_filename("Mutual NDA.docx") is False

    def test_return_detection(self) -> None:
        assert is_return_filename("NDA - redline.docx") is True
        assert is_return_filename("NDA returned.docx") is True
        assert is_return_filename("NDA fresh.docx") is False

    @pytest.mark.parametrize(
        ("filename", "expected"),
        [
            ("NDA_v3.docx", 3),
            ("Mutual NDA round 2.docx", 2),
            ("nda-rev-4.pdf", 4),
            ("NDA draft2.docx", 2),
            ("NDA.docx", None),
            ("NDA_v2 round3.docx", 3),
            ("MutualNDA_v3_countersigned.docx", 3),
            ("NDA_r4.docx", 4),
            ("version12_final.docx", 12),
        ],
    )
    def test_round_from_filename(self, filename: str, expected: int | None) -> None:
        assert round_from_filename(filename) == expected


# ---------------------------------------------------------------------------
# classify_mail_document
# ---------------------------------------------------------------------------


class TestClassifyMailDocument:
    def test_fresh_nda_root_message(self) -> None:
        role, rnd = classify_mail_document(
            filename="Mutual NDA.docx",
            subject="Project Falcon \u2014 Mutual NDA",
            conversation_index=_conv_index(0),
        )
        assert (role, rnd) == (ROLE_FRESH_NDA, 1)

    def test_teaser_on_fresh_send(self) -> None:
        role, rnd = classify_mail_document(
            filename="Project Teaser.pdf",
            subject="Project Falcon \u2014 Mutual NDA",
            conversation_index=_conv_index(0),
        )
        assert (role, rnd) == (ROLE_TEASER, 1)

    def test_counterparty_return_by_thread_depth(self) -> None:
        role, rnd = classify_mail_document(
            filename="Mutual NDA redline.docx",
            subject="RE: Project Falcon \u2014 Mutual NDA",
            conversation_index=_conv_index(1),
        )
        assert (role, rnd) == (ROLE_COUNTERPARTY_RETURN, 2)

    def test_second_return_increments_round(self) -> None:
        role, rnd = classify_mail_document(
            filename="NDA.docx",
            subject="RE: Mutual NDA",
            conversation_index=_conv_index(2),
        )
        assert (role, rnd) == (ROLE_COUNTERPARTY_RETURN, 3)

    def test_reply_prefix_without_index_still_return_round_two(self) -> None:
        # conversationIndex missing but subject says RE: → floor at round 2.
        role, rnd = classify_mail_document(
            filename="NDA.docx",
            subject="RE: Mutual NDA",
            conversation_index=None,
        )
        assert (role, rnd) == (ROLE_COUNTERPARTY_RETURN, 2)


# ---------------------------------------------------------------------------
# classify_sharepoint_document
# ---------------------------------------------------------------------------


class TestClassifySharepointDocument:
    def test_fresh_default(self) -> None:
        assert classify_sharepoint_document("MutualNDA_v1.docx") == (ROLE_FRESH_NDA, 1)

    def test_return_token(self) -> None:
        assert classify_sharepoint_document("MutualNDA_v2_return.docx") == (
            ROLE_COUNTERPARTY_RETURN,
            2,
        )

    def test_return_without_version_defaults_round_two(self) -> None:
        assert classify_sharepoint_document("NDA redline.docx") == (
            ROLE_COUNTERPARTY_RETURN,
            2,
        )

    def test_teaser(self) -> None:
        assert classify_sharepoint_document("Project Teaser.pdf") == (ROLE_TEASER, 1)


# ---------------------------------------------------------------------------
# sender_domain
# ---------------------------------------------------------------------------


class TestSenderDomain:
    def test_extracts_domain(self) -> None:
        msg = {"from": {"emailAddress": {"address": "Legal@Acme.com"}}}
        assert sender_domain(msg) == "acme.com"

    def test_missing_from(self) -> None:
        assert sender_domain({}) is None

    def test_malformed_address(self) -> None:
        assert sender_domain({"from": {"emailAddress": {"address": "not-an-email"}}}) is None
