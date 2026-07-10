"""Integration tests for m365_graph_source against a DuckDB destination.

All Microsoft Graph + identity-platform HTTP is mocked with ``responses``
(never pytest-httpx, per CONTRIBUTING.md).  The tests assert the source lands
documents into the ``documents`` dropzone table with the correct
``doc_role`` / ``round_no`` classification and normalised content, and that the
incremental ``cursor.start_value`` high-water mark is honoured on re-run.
"""

from __future__ import annotations

import base64

import responses

from paradox_dlt_sources.m365_graph import m365_graph_source
from tests._helpers.fixture_loader import load_fixture

_GRAPH = "https://graph.microsoft.com"
_LOGIN = "https://login.microsoftonline.com"
_TENANT = "test-tenant"
_SITE = "site-1"
_MAILBOX = "ndas@point41.com"

_MESSAGES_URL = f"{_GRAPH}/v1.0/users/{_MAILBOX}/messages"
_CHILDREN_URL = f"{_GRAPH}/v1.0/sites/{_SITE}/drive/root/children"


def _make_source() -> object:
    return m365_graph_source(
        tenant_id=_TENANT,
        client_id="client-id",
        client_secret="secret",
        site_id=_SITE,
        mailbox=_MAILBOX,
        graph_base_url=_GRAPH,
        login_base_url=_LOGIN,
    )


def _register_token(rsps: responses.RequestsMock) -> None:
    rsps.add(
        responses.POST,
        f"{_LOGIN}/{_TENANT}/oauth2/v2.0/token",
        json=load_fixture("m365_graph", "token_response"),
        status=200,
    )


def _register_mail(rsps: responses.RequestsMock) -> None:
    rsps.add(responses.GET, _MESSAGES_URL, json=load_fixture("m365_graph", "messages_page_1"))
    rsps.add(
        responses.GET,
        f"{_MESSAGES_URL}/msg-B/attachments",
        json=load_fixture("m365_graph", "attachments_msg_B"),
    )
    rsps.add(
        responses.GET,
        f"{_MESSAGES_URL}/msg-A/attachments",
        json=load_fixture("m365_graph", "attachments_msg_A"),
    )


def _register_sharepoint(rsps: responses.RequestsMock) -> None:
    rsps.add(responses.GET, _CHILDREN_URL, json=load_fixture("m365_graph", "sharepoint_children"))
    for item_id, body in (
        ("item-1", b"SP-FRESH"),
        ("item-2", b"SP-RETURN"),
        ("item-3", b"SP-TEASER"),
    ):
        rsps.add(
            responses.GET,
            f"{_GRAPH}/v1.0/sites/{_SITE}/drive/items/{item_id}/content",
            body=body,
            status=200,
        )


def _rows(pipeline) -> list[dict]:
    with pipeline.sql_client() as client:
        cols = [
            "document_id",
            "source_system",
            "doc_role",
            "round_no",
            "filename",
            "counterparty",
            "content_base64",
            "conversation_id",
            "received_at",
            "modified_at",
        ]
        res = client.execute_sql(
            f"SELECT {', '.join(cols)} FROM documents ORDER BY document_id"  # noqa: S608
        )
    return [dict(zip(cols, r, strict=True)) for r in res]


@responses.activate
def test_full_intake_classifies_and_lands(tmp_pipeline) -> None:
    """Both planes land into `documents` with correct doc_role/round_no."""
    _register_token(responses.mock)
    _register_mail(responses.mock)
    _register_sharepoint(responses.mock)

    info = tmp_pipeline.run(_make_source())
    assert not info.has_failed_jobs

    rows = _rows(tmp_pipeline)
    by_id = {r["document_id"]: r for r in rows}

    # msg-A (root): fresh NDA + teaser; inline image + itemAttachment dropped.
    fresh = by_id["outlook:msg-A:att-a1"]
    assert (fresh["doc_role"], fresh["round_no"]) == ("fresh_nda", 1)
    assert fresh["source_system"] == "outlook"
    assert fresh["counterparty"] == "point41.com"
    assert base64.b64decode(fresh["content_base64"]) == b"FRESH-NDA-BYTES"

    teaser = by_id["outlook:msg-A:att-a2"]
    assert (teaser["doc_role"], teaser["round_no"]) == ("teaser", 1)

    # msg-B (reply): counterparty return, round 2.
    ret = by_id["outlook:msg-B:att-b1"]
    assert (ret["doc_role"], ret["round_no"]) == ("counterparty_return", 2)
    assert ret["counterparty"] == "acme.com"

    # No row for the inline image, the itemAttachment, or msg-C (no attachments).
    assert "outlook:msg-A:att-a3" not in by_id
    assert "outlook:msg-A:att-a4" not in by_id
    assert not any(r["document_id"].startswith("outlook:msg-C") for r in rows)

    # SharePoint plane: fresh / return / teaser; the folder is skipped.
    sp_fresh = by_id[f"sharepoint:{_SITE}:item-1"]
    assert (sp_fresh["doc_role"], sp_fresh["round_no"]) == ("fresh_nda", 1)
    assert base64.b64decode(sp_fresh["content_base64"]) == b"SP-FRESH"

    sp_return = by_id[f"sharepoint:{_SITE}:item-2"]
    assert (sp_return["doc_role"], sp_return["round_no"]) == ("counterparty_return", 2)

    sp_teaser = by_id[f"sharepoint:{_SITE}:item-3"]
    assert sp_teaser["doc_role"] == "teaser"

    assert not any(r["document_id"].endswith("folder-1") for r in rows)

    # Six documents total: 3 from mail (fresh, teaser, return) + 3 SharePoint.
    assert len(rows) == 6


@responses.activate
def test_mail_only_resource_selection(tmp_pipeline) -> None:
    """Running just the mail resource lands mail documents and no SharePoint calls."""
    _register_token(responses.mock)
    _register_mail(responses.mock)

    source = _make_source()
    info = tmp_pipeline.run(source.mail_documents)
    assert not info.has_failed_jobs

    rows = _rows(tmp_pipeline)
    assert {r["source_system"] for r in rows} == {"outlook"}
    assert len(rows) == 3


@responses.activate
def test_incremental_uses_start_value_high_water_mark(tmp_pipeline) -> None:
    """A second run ingests only messages strictly newer than the prior mark.

    Run 1 seeds the cursor from the three-message fixture (high-water mark =
    msg-B's 2026-05-02 receipt).  Run 2 serves a page whose only new message
    post-dates that mark; the older messages (== / < mark) must not be
    re-ingested, proving `cursor.start_value` (not `last_value`) drives the
    early-termination floor.
    """
    _register_token(responses.mock)
    _register_mail(responses.mock)
    tmp_pipeline.run(_make_source().mail_documents)
    assert len(_rows(tmp_pipeline)) == 3

    # ── Run 2 ────────────────────────────────────────────────────────────
    new_message = {
        "id": "msg-D",
        "subject": "RE: Project Falcon \u2014 Mutual NDA",
        "receivedDateTime": "2026-06-01T09:00:00Z",  # newer than the mark
        "conversationId": "conv-1",
        "conversationIndex": base64.b64encode(b"\x01" * 32).decode(),  # depth 2
        "hasAttachments": True,
        "webLink": "https://outlook.office365.com/msg-D",
        "from": {"emailAddress": {"address": "legal@acme.com"}},
    }
    old_message = load_fixture("m365_graph", "messages_page_1")["value"][0]  # msg-B, == mark
    responses.mock.reset()
    _register_token(responses.mock)
    responses.mock.add(
        responses.GET,
        _MESSAGES_URL,
        json={"value": [new_message, old_message]},
    )
    responses.mock.add(
        responses.GET,
        f"{_MESSAGES_URL}/msg-D/attachments",
        json=load_fixture("m365_graph", "attachments_msg_B"),
    )

    info = tmp_pipeline.run(_make_source().mail_documents)
    assert not info.has_failed_jobs

    rows = _rows(tmp_pipeline)
    ids = [r["document_id"] for r in rows]
    # msg-D ingested once; msg-B not re-ingested (its receipt == prior mark).
    assert "outlook:msg-D:att-b1" in ids
    assert ids.count("outlook:msg-B:att-b1") == 1
    new_row = next(r for r in rows if r["document_id"] == "outlook:msg-D:att-b1")
    assert (new_row["doc_role"], new_row["round_no"]) == ("counterparty_return", 3)


@responses.activate
def test_sharepoint_incremental_skips_stale_items(tmp_pipeline) -> None:
    """SharePoint re-run skips items at/below the prior modified_at mark.

    The children listing is unsorted, so stale items are skipped individually
    (not early-terminated).  Run 2 adds a newer file and re-serves the original
    three; only the new file is ingested.
    """
    _register_token(responses.mock)
    _register_sharepoint(responses.mock)
    tmp_pipeline.run(_make_source().sharepoint_documents)
    assert len(_rows(tmp_pipeline)) == 3  # item-1/2/3; folder skipped

    # ── Run 2: original items (all <= mark) + one newer file ─────────────
    children = load_fixture("m365_graph", "sharepoint_children")
    children["value"].append(
        {
            "id": "item-4",
            "name": "MutualNDA_v3_countersigned.docx",
            "size": 7,
            "lastModifiedDateTime": "2026-07-01T12:00:00Z",  # newer than mark
            "webUrl": "https://point41.sharepoint.com/ndas/v3.docx",
            "file": {"mimeType": "application/vnd.openxmlformats"},
            "createdBy": {"user": {"email": "legal@acme.com"}},
        }
    )
    responses.mock.reset()
    _register_token(responses.mock)
    responses.mock.add(responses.GET, _CHILDREN_URL, json=children)
    responses.mock.add(
        responses.GET,
        f"{_GRAPH}/v1.0/sites/{_SITE}/drive/items/item-4/content",
        body=b"SP-V3",
        status=200,
    )

    info = tmp_pipeline.run(_make_source().sharepoint_documents)
    assert not info.has_failed_jobs

    rows = _rows(tmp_pipeline)
    ids = [r["document_id"] for r in rows]
    # Only the new item was fetched (no /content call for the stale three).
    assert f"sharepoint:{_SITE}:item-4" in ids
    assert ids.count(f"sharepoint:{_SITE}:item-1") == 1
    item4 = next(r for r in rows if r["document_id"] == f"sharepoint:{_SITE}:item-4")
    assert (item4["doc_role"], item4["round_no"]) == ("counterparty_return", 3)
