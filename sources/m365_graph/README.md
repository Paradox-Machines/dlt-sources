# m365_graph

Microsoft 365 / Graph dlt source — NDA intake from an Outlook shared mailbox
and a SharePoint document library into a single `documents` **dropzone** table
that `run_dropzone_poll` consumes.

Each inbound document is classified as a **fresh NDA**, a **counterparty
return**, or a **teaser**, and tagged with the negotiation `round_no`. Uses the
Microsoft Graph API with application (app-only) permissions — **no IMAP**.

## Resources

| Resource | Table | Primary key | Write disposition | Endpoint |
|---|---|---|---|---|
| `mail_documents` | `documents` | `document_id` | `merge` (incremental) | `GET /v1.0/users/{mailbox}/messages` → `/messages/{id}/attachments` |
| `sharepoint_documents` | `documents` | `document_id` | `merge` (incremental) | `GET /v1.0/sites/{site_id}/drive/root/children` → `/items/{id}/content` |

Both resources land into the same `documents` table so the poller has one
uniform surface. `document_id` is stable
(`outlook:{message_id}:{attachment_id}` / `sharepoint:{site_id}:{item_id}`), so
`merge` makes re-runs idempotent.

### Classification (`doc_role` + `round_no`)

**Mail** classifies by *thread position*. The root message of a conversation
carries the fresh outbound NDA (often alongside a teaser); any reply carries a
counterparty return. Depth is decoded from the Outlook `conversationIndex`
(22-byte header + 5 bytes per reply) and cross-checked against the subject reply
prefix (`RE:` / `FW:` and en/de/fr/nl/es variants). `round_no` is `1` for the
fresh send, `2` for the first return, and increments per reply. A teaser is
recognised by filename regardless of thread position.

**SharePoint** has no conversation context, so it classifies by filename
convention: teaser tokens first, then return/redline tokens
(`…return.docx`, `…redline.docx`, `…markup.docx`, …) mark a counterparty
return; everything else is a fresh NDA. An explicit `v3` / `round 2` / `rev-4`
token in the name sets `round_no` (a return defaults to round 2, a fresh draft
to round 1).

| `doc_role` | Meaning |
|---|---|
| `fresh_nda` | First outbound NDA draft (thread root / un-marked file) |
| `counterparty_return` | A reply / marked-up copy returned by the counterparty |
| `teaser` | Deal teaser shipped alongside a fresh NDA (not an NDA itself) |

### Incremental behaviour

Both resources compare against **`cursor.start_value`** — the stable
high-water mark from the *previous* run — **not** `cursor.last_value`, which
shifts mid-extract and would silently drop rows if a run is interrupted and
restarted (the repo-wide convention).

- `mail_documents` reads messages newest-first
  (`$orderby=receivedDateTime desc`) on `received_at` and **stops paging early**
  once a message's `receivedDateTime` drops to/below the mark.
- `sharepoint_documents` reads the (unsorted) drive-children listing on
  `modified_at` and **skips** items at/below the mark individually.

## Auth

App-only **OAuth 2.0 client-credentials** against the Microsoft identity
platform. Register an app in Entra ID, add a client secret, and grant these
**application** permissions with admin consent:

- `Mail.Read` — read the shared mailbox
- `Sites.Read.All` (or `Files.Read.All`) — read the SharePoint library

The source requests the static `https://graph.microsoft.com/.default` scope and
caches the access token in memory, refreshing before expiry.

## Config

`.dlt/secrets.toml`:

```toml
[sources.m365_graph]
tenant_id = "your-entra-tenant-id"      # GUID or verified domain
client_id = "your-app-client-id"
client_secret = "your-app-client-secret"
site_id = "your-sharepoint-site-id"
# mailbox defaults to ndas@point41.com; override if needed:
# mailbox = "ndas@point41.com"
```

The Graph and login hosts can be overridden with the `M365_GRAPH_API_BASE_URL`
and `M365_GRAPH_LOGIN_BASE_URL` env vars (national clouds / test mocks); the
mailbox can also be set with `M365_GRAPH_MAILBOX`.

## Example

```python
import dlt
from paradox_dlt_sources.m365_graph import m365_graph_source

pipeline = dlt.pipeline(
    pipeline_name="m365_graph_demo",
    destination="duckdb",
    dataset_name="m365_graph_data",
)
info = pipeline.run(m365_graph_source())
print(info)
```

To run only one plane:

```python
source = m365_graph_source()
pipeline.run(source.mail_documents)        # mailbox only
pipeline.run(source.sharepoint_documents)  # SharePoint only
```

## Dropzone schema

Every row carries the full column set (declared up front so dlt materialises
them even on all-NULL loads):

| Column | Notes |
|---|---|
| `document_id` | Stable primary key |
| `source_system` | `outlook` / `sharepoint` |
| `doc_role` | `fresh_nda` / `counterparty_return` / `teaser` |
| `round_no` | 1-based negotiation round |
| `filename`, `content_type`, `size_bytes` | Attachment / file metadata |
| `content_base64` | Document bytes, base64-encoded |
| `mailbox`, `message_id`, `attachment_id`, `conversation_id`, `subject`, `subject_normalized`, `received_at` | Mail-plane fields |
| `site_id`, `item_id`, `web_url`, `modified_at` | SharePoint-plane fields |
| `counterparty`, `sender_address` | Sender domain / address |

## Known limitations and edge cases

- **Classification is heuristic.** Thread depth from `conversationIndex` and
  filename tokens approximate the negotiation round; unusual subject lines,
  missing conversation headers, or non-conventional filenames may need tuning
  against real tenant data.
- **Inline images and non-file attachments** (`itemAttachment` /
  `referenceAttachment`, and `isInline` parts) are ignored — only
  `#microsoft.graph.fileAttachment` payloads are ingested.
- **Only file attachments carry `contentBytes`** inline; SharePoint file bytes
  are fetched from the per-item `/content` endpoint and base64-encoded so both
  planes store bytes identically.
- **Rate limiting** — Graph throttles with HTTP 429 + `Retry-After`. dlt's
  `RESTClient` does not auto-retry by default; add a retry policy for
  high-volume mailboxes/libraries.
