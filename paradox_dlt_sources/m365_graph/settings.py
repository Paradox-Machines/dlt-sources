"""Microsoft 365 / Graph source — endpoint constants and API configuration.

All Graph paths are versioned under ``/v1.0``.  The mail plane reads from a
single **shared mailbox** (``ndas@point41.com``) via the application-permission
``/users/{mailbox}`` addressing scheme, and the document plane reads from a
**SharePoint document library** exposed as a drive under ``/sites/{site_id}``.
"""

from __future__ import annotations

# --- Microsoft identity platform (OAuth 2.0 client-credentials) --------------
# Override the login host in tests via the M365_GRAPH_LOGIN_BASE_URL env var.
LOGIN_BASE_URL = "https://login.microsoftonline.com"

# The v2.0 token endpoint for a given tenant.  Formatted with the tenant id
# (GUID or verified domain) at client construction time.
PATH_TOKEN = "/{tenant_id}/oauth2/v2.0/token"

# App-only flows request the static ``.default`` scope: the union of the
# application permissions an admin has consented to for the app registration.
DEFAULT_SCOPE = "https://graph.microsoft.com/.default"

# Refresh the cached access token this many seconds before its stated expiry
# to avoid racing Graph's ~3600s TTL on long extracts.
TOKEN_REFRESH_SAFETY_SECONDS = 120

# --- Microsoft Graph ---------------------------------------------------------
# Override the Graph host in tests via the M365_GRAPH_API_BASE_URL env var.
GRAPH_API_BASE_URL = "https://graph.microsoft.com"
GRAPH_API_VERSION = "v1.0"

# Mail plane.  ``mailbox`` is the shared mailbox UPN (e.g. ndas@point41.com);
# application permissions let us address it directly under /users/{mailbox}.
# The shared NDA intake mailbox — overridable via M365_GRAPH_MAILBOX env var.
DEFAULT_MAILBOX = "ndas@point41.com"
PATH_MESSAGES = "/{version}/users/{mailbox}/messages"
PATH_MESSAGE_ATTACHMENTS = "/{version}/users/{mailbox}/messages/{message_id}/attachments"

# Document plane.  A SharePoint document library is the default drive of a site.
PATH_DRIVE_CHILDREN = "/{version}/sites/{site_id}/drive/root/children"
PATH_DRIVE_ITEM_CONTENT = "/{version}/sites/{site_id}/drive/items/{item_id}/content"

# Graph's ``@odata.type`` discriminator for file (vs item / reference) attachments.
FILE_ATTACHMENT_ODATA_TYPE = "#microsoft.graph.fileAttachment"

# Incremental cursor floor — the Unix epoch as an RFC-3339 string.  Graph
# ``receivedDateTime`` / ``lastModifiedDateTime`` values are RFC-3339 with a
# ``Z`` offset, so lexicographic comparison orders them correctly.
EPOCH_ISO = "1970-01-01T00:00:00Z"

# Page size for the mail list endpoint (Graph caps $top at 1000 for messages).
MESSAGES_PAGE_SIZE = 50

# --- Classification vocabulary ----------------------------------------------
# Document roles emitted on the `doc_role` column of the dropzone table.
ROLE_FRESH_NDA = "fresh_nda"
ROLE_COUNTERPARTY_RETURN = "counterparty_return"
ROLE_TEASER = "teaser"

# Source-system tags on the `source_system` column.
SYSTEM_OUTLOOK = "outlook"
SYSTEM_SHAREPOINT = "sharepoint"

# Filename tokens that mark an attachment/file as a deal teaser rather than an
# NDA itself.  A fresh NDA outbound "often ships with a teaser" — the teaser is
# classified separately so the dropzone poller does not treat it as an NDA.
TEASER_TOKENS = (
    "teaser",
    "teaser-deck",
    "cim",
    "one-pager",
    "onepager",
    "flyer",
    "overview",
)

# Filename tokens that mark a SharePoint file as a counterparty return (a marked
# up / redlined / countersigned copy) rather than the fresh outbound draft.
# The mail plane classifies by thread position instead; these are the fallback
# signal for the document plane, which has no conversation context.
RETURN_TOKENS = (
    "return",
    "returned",
    "redline",
    "redlined",
    "markup",
    "marked-up",
    "markedup",
    "counter",
    "countersigned",
    "revised",
    "comments",
    "tracked",
)

# HTTP status codes with special handling.
HTTP_NOT_FOUND = 404
