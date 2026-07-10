"""Microsoft 365 / Graph dlt source — NDA intake from Outlook + SharePoint.

This source feeds the NDA **dropzone** that ``run_dropzone_poll`` consumes.  It
reads two planes over the Microsoft Graph API (application permissions, no
IMAP) and normalises everything to a single ``documents`` table:

* **mail plane** (``mail_documents``) — file attachments on messages in the
  shared mailbox ``ndas@point41.com`` (``/users/{mailbox}/messages`` →
  ``/messages/{id}/attachments``).  Attachment bytes arrive inline as
  base64 ``contentBytes`` — no second round-trip.
* **document plane** (``sharepoint_documents``) — files in a SharePoint
  document library (``/sites/{site_id}/drive/root/children``), with content
  fetched from the per-item ``/content`` endpoint.

Classification (``doc_role`` + ``round_no``)
--------------------------------------------
Every inbound is classified as one of ``fresh_nda`` / ``counterparty_return`` /
``teaser``:

* **Mail** uses *thread position*.  The root message of a conversation carries
  the fresh outbound NDA (often alongside a **teaser**); any reply carries a
  counterparty return.  Depth is decoded from the Outlook ``conversationIndex``
  (22-byte header + 5 bytes/reply) and cross-checked against the subject reply
  prefix (``RE:`` / ``FW:`` / locale variants).  ``round_no`` is 1 for the fresh
  send, 2 for the first return, and so on.
* **SharePoint** has no thread context, so it classifies by filename
  convention: teaser tokens first, then return/redline tokens
  (``…return.docx``, ``…redline.docx``); an explicit ``v3`` / ``round 2`` token
  sets ``round_no``.

Incremental
-----------
Both resources are incremental and compare against ``cursor.start_value`` — the
stable high-water mark from the *previous* run — **not** ``cursor.last_value``,
which shifts mid-extract and would silently drop rows if a run is interrupted
(the repo-wide convention; see ``paradox_dlt_sources/notion``).  ``mail_documents``
reads messages newest-first and early-terminates once ``receivedDateTime`` drops
to/below the mark; ``sharepoint_documents`` skips items at/below the mark
(the drive children listing is unsorted).

Column hints
------------
Every column is declared up front (see ``helpers.columns``) so dlt always
materialises the full dropzone schema even on an all-NULL / zero-row load.

Auth
----
App-only OAuth 2.0 client-credentials against the Microsoft identity platform
(``ClientCredentialsAuth``).  Configure ``.dlt/secrets.toml`` →
``[sources.m365_graph]`` with ``tenant_id`` / ``client_id`` / ``client_secret``
/ ``site_id``; ``mailbox`` defaults to the shared NDA mailbox.  The app
registration needs ``Mail.Read`` + ``Sites.Read.All`` application permissions
with admin consent.
"""

from __future__ import annotations

import base64
import logging
import os
from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    ClientCredentialsAuth,
    classify_mail_document,
    classify_sharepoint_document,
    columns,
    make_client,
    normalize_subject,
    sender_domain,
)
from .settings import (
    DEFAULT_MAILBOX,
    EPOCH_ISO,
    FILE_ATTACHMENT_ODATA_TYPE,
    GRAPH_API_BASE_URL,
    GRAPH_API_VERSION,
    LOGIN_BASE_URL,
    MESSAGES_PAGE_SIZE,
    PATH_DRIVE_CHILDREN,
    PATH_DRIVE_ITEM_CONTENT,
    PATH_MESSAGE_ATTACHMENTS,
    PATH_MESSAGES,
    SYSTEM_OUTLOOK,
    SYSTEM_SHAREPOINT,
)

logger = logging.getLogger(__name__)

Row = dict[str, Any]

# The dropzone table.  Both planes land here so run_dropzone_poll has one
# uniform surface regardless of whether a document arrived by mail or SharePoint.
DROPZONE_TABLE = "documents"

# Message fields requested from Graph — enough to classify and attribute without
# over-fetching bodies.
_MESSAGE_SELECT = (
    "id,subject,receivedDateTime,conversationId,conversationIndex,hasAttachments,from,webLink"
)

_DOCUMENT_COLUMNS = columns(
    text=(
        "document_id",
        "source_system",
        "mailbox",
        "site_id",
        "message_id",
        "attachment_id",
        "item_id",
        "conversation_id",
        "subject",
        "subject_normalized",
        "doc_role",
        "counterparty",
        "sender_address",
        "filename",
        "content_type",
        "web_url",
        "content_base64",
    ),
    bigint=(
        "round_no",
        "size_bytes",
    ),
    timestamp=(
        "received_at",
        "modified_at",
    ),
)


def _base_row() -> Row:
    """Return a document row pre-populated with every column as ``None``.

    Both planes emit into the same ``documents`` table; starting from a full
    NULL template keeps the two row shapes identical so dlt sees a single stable
    schema and the dropzone poller can rely on every column existing.
    """
    return {
        "document_id": None,
        "source_system": None,
        "mailbox": None,
        "site_id": None,
        "message_id": None,
        "attachment_id": None,
        "item_id": None,
        "conversation_id": None,
        "subject": None,
        "subject_normalized": None,
        "doc_role": None,
        "round_no": None,
        "counterparty": None,
        "sender_address": None,
        "filename": None,
        "content_type": None,
        "size_bytes": None,
        "web_url": None,
        "content_base64": None,
        "received_at": None,
        "modified_at": None,
    }


@dlt.source(name="m365_graph")
def m365_graph_source(
    tenant_id: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    site_id: str = dlt.config.value,
    mailbox: str = os.environ.get("M365_GRAPH_MAILBOX", DEFAULT_MAILBOX),
    graph_base_url: str = os.environ.get("M365_GRAPH_API_BASE_URL", GRAPH_API_BASE_URL),
    login_base_url: str = os.environ.get("M365_GRAPH_LOGIN_BASE_URL", LOGIN_BASE_URL),
) -> list[Any]:
    """Microsoft 365 / Graph source — NDA intake into the dropzone ``documents`` table.

    Args:
        tenant_id: Entra tenant id (GUID) or verified domain.
        client_id: App registration (client) id.
        client_secret: App registration client secret.
        site_id: SharePoint site id backing the NDA document library.
        mailbox: Shared mailbox UPN. Defaults to ``ndas@point41.com``.
        graph_base_url: Graph API host. Override in tests to a mock server.
        login_base_url: Microsoft identity platform host. Override in tests.
    """
    auth = ClientCredentialsAuth(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        login_base_url=login_base_url,
    )
    client = make_client(auth, base_url=graph_base_url)

    @dlt.resource(
        name="mail_documents",
        table_name=DROPZONE_TABLE,
        primary_key="document_id",
        write_disposition="merge",
        columns=_DOCUMENT_COLUMNS,
    )
    def mail_documents(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "received_at",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        """File attachments from the shared NDA mailbox, classified by thread.

        Messages are read newest-first (``$orderby=receivedDateTime desc``).
        Because the feed is descending, once a message's ``receivedDateTime``
        falls to/below ``cursor.start_value`` — the prior run's high-water mark
        — every later message is older too, so paging stops early.
        ``start_value`` (not ``last_value``) is used deliberately: ``last_value``
        advances during this run and would skip messages ingested earlier in the
        same run if it were interrupted and restarted.
        """
        threshold = cursor.start_value
        stop = False
        for page in client.paginate(
            PATH_MESSAGES.format(version=GRAPH_API_VERSION, mailbox=mailbox),
            params={
                "$orderby": "receivedDateTime desc",
                "$top": MESSAGES_PAGE_SIZE,
                "$select": _MESSAGE_SELECT,
            },
        ):
            for message in page:
                if message.get("receivedDateTime", "") <= threshold:
                    stop = True
                    break
                if not message.get("hasAttachments"):
                    continue
                yield from _mail_document_rows(client, mailbox, message)
            if stop:
                break

    @dlt.resource(
        name="sharepoint_documents",
        table_name=DROPZONE_TABLE,
        primary_key="document_id",
        write_disposition="merge",
        columns=_DOCUMENT_COLUMNS,
    )
    def sharepoint_documents(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "modified_at",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        """Files from the SharePoint NDA library, classified by filename.

        The drive children listing is **not** sorted by modification time, so
        (unlike ``mail_documents``) stale items at/below ``cursor.start_value``
        are *skipped* rather than triggering early termination.  ``start_value``
        is the prior run's stable high-water mark — see ``mail_documents`` for
        why ``last_value`` is unsafe here.
        """
        threshold = cursor.start_value
        for page in client.paginate(
            PATH_DRIVE_CHILDREN.format(version=GRAPH_API_VERSION, site_id=site_id),
        ):
            for item in page:
                # Skip folders and other non-file driveItems (no `file` facet).
                if "file" not in item:
                    continue
                if item.get("lastModifiedDateTime", "") <= threshold:
                    continue
                yield _sharepoint_document_row(client, site_id, item)

    return [mail_documents, sharepoint_documents]


def _mail_document_rows(client: RESTClient, mailbox: str, message: Row) -> Iterator[Row]:
    """Yield one dropzone row per non-inline file attachment on *message*.

    Item attachments, reference (cloud) attachments, and inline images are
    ignored — only ``#microsoft.graph.fileAttachment`` payloads carry an NDA or
    teaser document.  ``contentBytes`` is already base64 on the attachment, so
    no additional download call is made.
    """
    message_id = message["id"]
    subject = message.get("subject")
    conversation_index = message.get("conversationIndex")
    for page in client.paginate(
        PATH_MESSAGE_ATTACHMENTS.format(
            version=GRAPH_API_VERSION, mailbox=mailbox, message_id=message_id
        ),
    ):
        for attachment in page:
            if attachment.get("@odata.type") != FILE_ATTACHMENT_ODATA_TYPE:
                continue
            if attachment.get("isInline"):
                continue
            filename = attachment.get("name") or "attachment"
            doc_role, round_no = classify_mail_document(
                filename=filename,
                subject=subject,
                conversation_index=conversation_index,
            )
            row = _base_row()
            row.update(
                {
                    "document_id": f"{SYSTEM_OUTLOOK}:{message_id}:{attachment['id']}",
                    "source_system": SYSTEM_OUTLOOK,
                    "mailbox": mailbox,
                    "message_id": message_id,
                    "attachment_id": attachment["id"],
                    "conversation_id": message.get("conversationId"),
                    "subject": subject,
                    "subject_normalized": normalize_subject(subject),
                    "doc_role": doc_role,
                    "round_no": round_no,
                    "counterparty": sender_domain(message),
                    "sender_address": (message.get("from") or {})
                    .get("emailAddress", {})
                    .get("address")
                    if isinstance(message.get("from"), dict)
                    else None,
                    "filename": filename,
                    "content_type": attachment.get("contentType"),
                    "size_bytes": attachment.get("size"),
                    "web_url": message.get("webLink"),
                    "content_base64": attachment.get("contentBytes"),
                    "received_at": message.get("receivedDateTime"),
                }
            )
            yield row


def _sharepoint_document_row(client: RESTClient, site_id: str, item: Row) -> Row:
    """Build a dropzone row for a SharePoint driveItem, downloading its bytes.

    The item's content is fetched from ``/drive/items/{id}/content`` and
    base64-encoded so both planes store bytes the same way (mail attachments
    arrive pre-encoded).
    """
    filename = item.get("name") or "file"
    doc_role, round_no = classify_sharepoint_document(filename)
    content = client.get(
        PATH_DRIVE_ITEM_CONTENT.format(
            version=GRAPH_API_VERSION, site_id=site_id, item_id=item["id"]
        )
    )
    content.raise_for_status()
    created_by = item.get("createdBy") or {}
    user = created_by.get("user", {}) if isinstance(created_by, dict) else {}
    row = _base_row()
    row.update(
        {
            "document_id": f"{SYSTEM_SHAREPOINT}:{site_id}:{item['id']}",
            "source_system": SYSTEM_SHAREPOINT,
            "site_id": site_id,
            "item_id": item["id"],
            "doc_role": doc_role,
            "round_no": round_no,
            "counterparty": user.get("email"),
            "sender_address": user.get("email"),
            "filename": filename,
            "content_type": (item.get("file") or {}).get("mimeType"),
            "size_bytes": item.get("size"),
            "web_url": item.get("webUrl"),
            "content_base64": base64.b64encode(content.content).decode("ascii"),
            "modified_at": item.get("lastModifiedDateTime"),
        }
    )
    return row


__all__ = ["m365_graph_source"]
