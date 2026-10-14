"""ServiceTitan source — endpoint table and API constants.

Every ServiceTitan v2 path is tenant-scoped:
``/{module}/v2/tenant/{tenant}/{path}``.  Two pagination styles exist:

* ``export`` — ``/export/*`` endpoints.  Each response carries
  ``{"data": [...], "hasMore": bool, "continueFrom": "<token>"}``; the next
  request passes the token back as ``from``.  The final token is persisted so
  the next run only receives records changed since.
* ``page`` — classic list endpoints.  ``page`` (1-based) / ``pageSize``
  params, ``{"data": [...], "hasMore": bool}`` responses.  Where the endpoint
  supports ``modifiedOnOrAfter`` the resource is incremental on ``modifiedOn``.

Reference: https://developer.servicetitan.io/apis/
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

# --- Auth (OAuth 2.0 client-credentials) -------------------------------------
# Override in tests via the SERVICETITAN_AUTH_BASE_URL env var.
AUTH_BASE_URL = "https://auth.servicetitan.io"
PATH_TOKEN = "/connect/token"

# Refresh the cached bearer token this many seconds before its stated expiry.
# ServiceTitan tokens live ~900s, so a long tenant extract crosses expiry.
TOKEN_REFRESH_SAFETY_SECONDS = 60
DEFAULT_TOKEN_TTL_SECONDS = 900

APP_KEY_HEADER = "ST-App-Key"

# --- API -----------------------------------------------------------------------
# Override in tests via the SERVICETITAN_API_BASE_URL env var.
API_BASE_URL = "https://api.servicetitan.io"
API_VERSION = "v2"
PATH_TEMPLATE = "/{module}/{version}/tenant/{tenant}/{path}"

DATA_SELECTOR = "data"
DEFAULT_PAGE_SIZE = 500

# Attempts per request (429 / 5xx / connection errors).  429s wait for the
# server's ``Retry-After`` header.
DEFAULT_MAX_ATTEMPTS = 8
MAX_RETRY_DELAY_SECONDS = 120

HTTP_FORBIDDEN = 403

Pagination = Literal["export", "page"]

MODULE_LABELS: Mapping[str, str] = {
    "jpm": "Job Planning & Management",
    "crm": "CRM",
    "accounting": "Accounting",
    "inventory": "Inventory",
    "sales": "Sales & Estimates",
    "settings": "Settings",
    "forms": "Forms",
}


@dataclass(frozen=True)
class Endpoint:
    """One ServiceTitan collection loaded as one table.

    Attributes:
        name: Resource / table name.
        module: API module (first path segment, e.g. ``jpm``).
        path: Path below ``/tenant/{tenant}/``.
        pagination: ``export`` (continuation token) or ``page`` (page/hasMore).
        scope: Human-readable ServiceTitan app scope needed (used in the
            403 skip warning and the README).
        incremental: ``page`` endpoints only — filter server-side with
            ``modifiedOnOrAfter`` and track ``modifiedOn``.
        params: Fixed query params sent on every request.
        columns: Extra column hints (snake_case name -> dlt data type).
    """

    name: str
    module: str
    path: str
    pagination: Pagination
    scope: str
    incremental: bool = False
    params: Mapping[str, str] = field(default_factory=dict)
    columns: Mapping[str, str] = field(default_factory=dict)


# Column hints every table carries, so tenant tagging and the compound key
# always materialise even on a zero-row load.
BASE_COLUMNS: Mapping[str, str] = {
    "tenant_id": "text",
    "tenant_name": "text",
    "id": "bigint",
    "modified_on": "timestamp",
}

PRIMARY_KEY = ("tenant_id", "id")

INCREMENTAL_CURSOR = "modifiedOn"
INCREMENTAL_PARAM = "modifiedOnOrAfter"
EXPORT_FROM_PARAM = "from"

ENDPOINTS: tuple[Endpoint, ...] = (
    # --- jpm ---------------------------------------------------------------
    Endpoint("jobs", "jpm", "export/jobs", "export", "Job Planning & Management: Jobs (read)"),
    Endpoint(
        "appointments",
        "jpm",
        "export/appointments",
        "export",
        "Job Planning & Management: Appointments (read)",
    ),
    Endpoint(
        "projects", "jpm", "export/projects", "export", "Job Planning & Management: Projects (read)"
    ),
    Endpoint(
        "job_cancel_reasons",
        "jpm",
        "export/job-cancel-reasons",
        "export",
        "Job Planning & Management: Job Cancel Reasons (read)",
    ),
    Endpoint(
        "job_types",
        "jpm",
        "job-types",
        "page",
        "Job Planning & Management: Job Types (read)",
        incremental=True,
        params={"active": "Any"},
    ),
    # --- crm ---------------------------------------------------------------
    Endpoint("customers", "crm", "export/customers", "export", "CRM: Customers (read)"),
    Endpoint("locations", "crm", "export/locations", "export", "CRM: Locations (read)"),
    Endpoint(
        "contacts",
        "crm",
        "export/customers/contacts",
        "export",
        "CRM: Customers (read)",
        columns={"customer_id": "bigint"},
    ),
    Endpoint("bookings", "crm", "export/bookings", "export", "CRM: Bookings (read)"),
    # --- accounting ----------------------------------------------------------
    # ``items`` lands in the ``invoices__items`` child table.
    Endpoint("invoices", "accounting", "export/invoices", "export", "Accounting: Invoices (read)"),
    Endpoint("payments", "accounting", "export/payments", "export", "Accounting: Payments (read)"),
    # Inventory bills are ServiceTitan's AP bills (vendor bills against POs).
    Endpoint(
        "inventory_bills",
        "accounting",
        "export/inventory-bills",
        "export",
        "Accounting: Inventory Bills (read)",
    ),
    # --- inventory -----------------------------------------------------------
    # ``items`` lands in the ``purchase_orders__items`` child table.
    Endpoint(
        "purchase_orders",
        "inventory",
        "export/purchase-orders",
        "export",
        "Inventory: Purchase Orders (read)",
    ),
    Endpoint(
        "vendors",
        "inventory",
        "vendors",
        "page",
        "Inventory: Vendors (read)",
        incremental=True,
        params={"active": "Any"},
    ),
    # --- sales ---------------------------------------------------------------
    # ``items`` lands in the ``estimates__items`` child table.
    Endpoint(
        "estimates", "sales", "export/estimates", "export", "Sales & Estimates: Estimates (read)"
    ),
    # --- settings ------------------------------------------------------------
    Endpoint(
        "business_units",
        "settings",
        "export/business-units",
        "export",
        "Settings: Business Units (read)",
    ),
    Endpoint(
        "technicians",
        "settings",
        "export/technicians",
        "export",
        "Settings: Technicians (read)",
        columns={
            "team": "text",
            "business_unit_id": "bigint",
            "main_zone_id": "bigint",
            "zone_ids": "json",
            "role_ids": "json",
            "skills": "json",
        },
    ),
    Endpoint("employees", "settings", "export/employees", "export", "Settings: Employees (read)"),
    # --- forms ---------------------------------------------------------------
    # ``units`` holds the filled-in form tree (refrigerant usage, parts, unit
    # details); it lands as one JSON column so arbitrary form layouts never
    # explode into per-section child tables.
    Endpoint(
        "form_submissions",
        "forms",
        "submissions",
        "page",
        "Forms: Submissions (read)",
        columns={
            "form_id": "bigint",
            "form_name": "text",
            "submitted_on": "timestamp",
            "units": "json",
            "owners": "json",
        },
    ),
)

ENDPOINTS_BY_NAME: Mapping[str, Endpoint] = {e.name: e for e in ENDPOINTS}
