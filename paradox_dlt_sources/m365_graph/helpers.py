"""Microsoft 365 / Graph source helpers.

Contains:

- ``ClientCredentialsAuth`` — OAuth 2.0 app-only (client-credentials) grant
  against the Microsoft identity platform, with in-memory access-token caching.
- ``graph_paginator`` / ``make_client`` — a ``RESTClient`` wired for Graph's
  ``@odata.nextLink`` pagination envelope.
- ``columns`` / ``nullable_column`` — schema-hint builders (mirror the attio /
  notion / quickbooks pattern; dlt drops all-NULL columns otherwise).
- Classification helpers (``conversation_depth``, ``subject_is_reply``,
  ``classify_mail_document``, ``classify_sharepoint_document``, …) that decide
  each inbound document's ``doc_role`` and negotiation ``round_no``.

Every source here is self-contained (see AGENTS.md) — no cross-source imports.
"""

from __future__ import annotations

import base64
import binascii
import re
import time
from typing import Any

import requests
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from requests import PreparedRequest
from requests.auth import AuthBase

from .settings import (
    DEFAULT_SCOPE,
    GRAPH_API_BASE_URL,
    PATH_TOKEN,
    RETURN_TOKENS,
    ROLE_COUNTERPARTY_RETURN,
    ROLE_FRESH_NDA,
    ROLE_TEASER,
    TEASER_TOKENS,
    TOKEN_REFRESH_SAFETY_SECONDS,
)

Row = dict[str, Any]

# Outlook / Exchange conversationIndex layout: a 22-byte header identifies the
# root message; every reply appends a 5-byte "child block".  The depth of a
# message in its thread is therefore (len_bytes - 22) // 5.  See MS-OXOMSG
# §2.2.1.3.
_CONV_INDEX_HEADER_BYTES = 22
_CONV_INDEX_CHILD_BYTES = 5

# The first counterparty return is negotiation round 2 (the fresh outbound is
# round 1).  A reply whose thread depth is unknown is floored to this round.
_FIRST_RETURN_ROUND = 2

# Reply/forward subject prefixes across the locales point41 counterparties use
# (en/de/fr/nl/es).  Matched case-insensitively and stripped iteratively so
# "RE: FW: RE:" collapses to the bare subject for reference matching.
_REPLY_PREFIX_RE = re.compile(
    r"^\s*(re|fw|fwd|aw|wg|antw|tr|rv|res|sv|vs)\s*(\[\d+\])?\s*:\s*",
    re.IGNORECASE,
)

# Version/round tokens embedded in filenames, e.g. "NDA_v3.docx",
# "MutualNDA round 2.docx", "nda-r4-return.pdf", "NDA draft2.docx".  A leading
# separator (or string start) anchors the keyword so "_v3" matches (``\b`` does
# not fire between the word chars ``_`` and ``v``); a trailing ``(?!\d)`` stops
# a longer number being truncated without requiring a word boundary before the
# word-char ``_`` in names like "NDA_v3_return.docx".
_ROUND_RE = re.compile(
    r"(?:^|[ _\-.])(?:version|round|draft|iteration|iter|rev|v|r)[ _\-.]*(\d{1,3})(?!\d)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# OAuth 2.0 — client-credentials (app-only) grant
# ---------------------------------------------------------------------------


class ClientCredentialsAuth(AuthBase):
    """Microsoft identity platform app-only auth (OAuth 2.0 client-credentials).

    Exchanges ``(tenant_id, client_id, client_secret)`` for an app-only access
    token at ``/{tenant_id}/oauth2/v2.0/token`` requesting the static
    ``https://graph.microsoft.com/.default`` scope, caches it in memory, and
    attaches it as ``Authorization: Bearer <token>`` on every outgoing request.
    The token is refreshed automatically once it is within
    ``TOKEN_REFRESH_SAFETY_SECONDS`` of the server-stated expiry.

    App-only (rather than delegated) auth is required to read a **shared**
    mailbox and a SharePoint library headlessly — there is no interactive user.
    Grant the app registration ``Mail.Read`` and ``Sites.Read.All`` (or
    ``Files.Read.All``) application permissions with admin consent.

    Args:
        tenant_id: Entra tenant id (GUID) or a verified domain.
        client_id: Application (client) id of the app registration.
        client_secret: A client secret for the app registration.
        login_base_url: Microsoft identity platform host. Defaults to the public
            cloud; override for national clouds or to point tests at a mock.
        scope: OAuth scope. Defaults to Graph's ``.default``.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        login_base_url: str,
        scope: str = DEFAULT_SCOPE,
    ) -> None:
        self._tenant_id = tenant_id.strip()
        self._client_id = client_id.strip()
        self._client_secret = client_secret
        self._scope = scope
        self._token_url = (
            f"{login_base_url.rstrip('/')}{PATH_TOKEN.format(tenant_id=self._tenant_id)}"
        )
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
        """Fetch and cache a fresh app-only access token."""
        response = requests.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scope,
            },
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        self._access_token = body["access_token"]
        self._expires_at = time.time() + float(body.get("expires_in", 3600))


# ---------------------------------------------------------------------------
# REST client / pagination
# ---------------------------------------------------------------------------


def graph_paginator() -> JSONLinkPaginator:
    """Paginator for Graph list endpoints.

    Graph returns ``{ "value": [...], "@odata.nextLink": "<absolute url>" }``;
    the next-page link is an absolute URL carrying an opaque ``$skiptoken``.
    The ``@odata.nextLink`` key contains a dot, so the JSONPath is quoted to
    stop it being parsed as a two-segment ``@odata`` → ``nextLink`` path.
    """
    return JSONLinkPaginator(next_url_path='"@odata.nextLink"')


def make_client(auth: AuthBase, base_url: str = GRAPH_API_BASE_URL) -> RESTClient:
    """Return a ``RESTClient`` pre-configured for Microsoft Graph.

    Args:
        auth: A ``requests`` ``AuthBase`` — normally ``ClientCredentialsAuth``.
        base_url: Graph host. Override in tests to point at a mock server.
    """
    return RESTClient(
        base_url=base_url,
        auth=auth,
        paginator=graph_paginator(),
        data_selector="value",
    )


# ---------------------------------------------------------------------------
# Schema-hint builders (same pattern as attio / notion / quickbooks helpers)
# ---------------------------------------------------------------------------


def nullable_column(data_type: str) -> dict[str, Any]:
    """Return a single nullable dlt column-hint dict of the given data type."""
    return {"data_type": data_type, "nullable": True}


def columns(
    *,
    text: tuple[str, ...] = (),
    bigint: tuple[str, ...] = (),
    timestamp: tuple[str, ...] = (),
) -> dict[str, dict[str, Any]]:
    """Build a ``@dlt.resource(columns=…)`` map of nullable hints.

    dlt drops all-NULL columns from a load batch's schema, which then breaks
    the downstream dropzone poller that references them by name.  Up-front
    hints guarantee every column materialises regardless of data shape.  Each
    column gets its own dict instance — dlt mutates these (assigns ``name``),
    so sharing one dict across columns silently merges them.

    Args:
        text: columns stored as text/varchar.
        bigint: columns stored as 64-bit integers (e.g. byte sizes).
        timestamp: columns for RFC-3339 datetime strings.
    """
    out: dict[str, dict[str, Any]] = {}
    for c in text:
        out[c] = nullable_column("text")
    for c in bigint:
        out[c] = nullable_column("bigint")
    for c in timestamp:
        out[c] = nullable_column("timestamp")
    return out


# ---------------------------------------------------------------------------
# Classification — fresh-NDA vs counterparty-return, and negotiation round
# ---------------------------------------------------------------------------


def conversation_depth(conversation_index: str | None) -> int:
    """Return a message's 0-based depth in its Outlook conversation thread.

    Decodes the base64 ``conversationIndex`` and derives depth from its length:
    a 22-byte header identifies the root (depth 0) and each reply appends a
    5-byte block.  Returns ``0`` when the header is missing, malformed, or too
    short — callers combine this with the subject-prefix signal so a missing
    index never misclassifies a reply as fresh.

    Args:
        conversation_index: The base64 ``conversationIndex`` header, or ``None``.
    """
    if not conversation_index:
        return 0
    try:
        raw = base64.b64decode(conversation_index, validate=True)
    except (binascii.Error, ValueError):
        return 0
    if len(raw) <= _CONV_INDEX_HEADER_BYTES:
        return 0
    return (len(raw) - _CONV_INDEX_HEADER_BYTES) // _CONV_INDEX_CHILD_BYTES


def subject_is_reply(subject: str | None) -> bool:
    """Return True when *subject* carries a reply/forward prefix (RE:, FW:, …)."""
    return bool(subject and _REPLY_PREFIX_RE.match(subject))


def normalize_subject(subject: str | None) -> str:
    """Strip leading reply/forward prefixes so threads can be matched by subject.

    Iteratively removes ``RE:`` / ``FW:`` / locale variants and collapses
    whitespace, e.g. ``"RE: FW:  Mutual NDA "`` → ``"mutual nda"``.  Used to
    group a fresh outbound and its counterparty returns onto the same thread key
    when ``conversationId`` is unavailable.
    """
    if not subject:
        return ""
    text = subject
    while True:
        stripped = _REPLY_PREFIX_RE.sub("", text, count=1)
        if stripped == text:
            break
        text = stripped
    return " ".join(text.split()).casefold()


def _filename_has_token(filename: str, tokens: tuple[str, ...]) -> bool:
    """Return True when the lower-cased filename contains any of *tokens*."""
    lowered = filename.casefold()
    return any(tok in lowered for tok in tokens)


def is_teaser_filename(filename: str) -> bool:
    """Return True when the filename looks like a deal teaser, not an NDA."""
    return _filename_has_token(filename, TEASER_TOKENS)


def is_return_filename(filename: str) -> bool:
    """Return True when the filename looks like a marked-up counterparty return."""
    return _filename_has_token(filename, RETURN_TOKENS)


def round_from_filename(filename: str) -> int | None:
    """Extract an explicit version/round number from a filename, else ``None``.

    Matches ``v3`` / ``round 2`` / ``rev-4`` / ``draft2`` style tokens and
    returns the largest one found (a "NDA_v2 round3" name is round 3).
    """
    matches = [int(m.group(1)) for m in _ROUND_RE.finditer(filename)]
    return max(matches) if matches else None


def _round_from_thread(*, is_reply: bool, depth: int) -> int:
    """Map a thread position to a 1-based negotiation round.

    Fresh outbound (depth 0, not a reply) is round 1; the first counterparty
    return is round 2; and so on.  ``depth`` from ``conversationIndex`` is the
    primary signal, with the subject prefix as a floor so a reply whose index
    is missing still lands on at least round 2.
    """
    base = depth + 1
    if is_reply and base < _FIRST_RETURN_ROUND:
        return _FIRST_RETURN_ROUND
    return base


def classify_mail_document(
    *,
    filename: str,
    subject: str | None,
    conversation_index: str | None,
) -> tuple[str, int]:
    """Classify a mailbox attachment into ``(doc_role, round_no)``.

    Thread position is the deciding signal: the root message of a conversation
    carries the **fresh NDA** outbound (often alongside a teaser), while any
    reply carries a **counterparty return**.  Depth is read from
    ``conversationIndex`` and cross-checked against the subject's reply prefix.
    A teaser is recognised by filename regardless of thread position and tagged
    with its own role so the poller does not ingest it as an NDA.

    Args:
        filename: The attachment filename.
        subject: The parent message subject line.
        conversation_index: The message's base64 ``conversationIndex`` header.

    Returns:
        ``(doc_role, round_no)`` — ``doc_role`` is one of ``fresh_nda`` /
        ``counterparty_return`` / ``teaser``; ``round_no`` is the 1-based
        negotiation round the document belongs to.
    """
    depth = conversation_depth(conversation_index)
    is_reply = depth > 0 or subject_is_reply(subject)
    round_no = _round_from_thread(is_reply=is_reply, depth=depth)
    if is_teaser_filename(filename):
        return ROLE_TEASER, round_no
    if is_reply:
        return ROLE_COUNTERPARTY_RETURN, round_no
    return ROLE_FRESH_NDA, round_no


def classify_sharepoint_document(filename: str) -> tuple[str, int]:
    """Classify a SharePoint library file into ``(doc_role, round_no)``.

    The document plane has no conversation context, so classification falls back
    to filename convention: teaser tokens win first, then return/redline tokens
    mark a **counterparty return**; anything else is treated as a **fresh NDA**.
    An explicit ``v``/``round`` token in the name sets ``round_no`` (a return
    defaults to round 2, a fresh draft to round 1 when no token is present).

    Args:
        filename: The driveItem filename.

    Returns:
        ``(doc_role, round_no)`` — see :func:`classify_mail_document`.
    """
    explicit = round_from_filename(filename)
    if is_teaser_filename(filename):
        return ROLE_TEASER, explicit or 1
    if is_return_filename(filename):
        return ROLE_COUNTERPARTY_RETURN, explicit or _FIRST_RETURN_ROUND
    return ROLE_FRESH_NDA, explicit or 1


def sender_domain(message: Row) -> str | None:
    """Return the sender's email domain (the counterparty) from a Graph message.

    Reads ``from.emailAddress.address`` and returns the substring after ``@``,
    lower-cased.  Returns ``None`` when the address is absent or malformed.
    """
    address = (
        (message.get("from") or {}).get("emailAddress", {}).get("address")
        if isinstance(message.get("from"), dict)
        else None
    )
    if not address or "@" not in address:
        return None
    return address.rsplit("@", 1)[-1].casefold() or None
