"""QuickBooks Online source — endpoint constants and resource lists."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Base URLs
# ---------------------------------------------------------------------------

# Production Intuit API base. Override via env var in tests / sandbox.
QUICKBOOKS_API_BASE_URL = "https://quickbooks.api.intuit.com"

# OAuth 2.0 token endpoint (used by RotatingRefreshTokenAuth).
QUICKBOOKS_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# ---------------------------------------------------------------------------
# Query layer constants
# ---------------------------------------------------------------------------

# QBO query MAXRESULTS ceiling. QBO hard-caps at 1000.
MAX_RESULTS = 1000

# All QBO entity queries go to /v3/company/{realmId}/query. The minor version
# is appended as a query param to every request.
QUERY_PATH_TEMPLATE = "/v3/company/{realm_id}/query"
MINOR_VERSION = "65"

# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

# 18 incremental resources — cursor is MetaData.LastUpdatedTime (ISO-8601+tz).
# Tuple of (QBO entity name, dlt resource name).
INCREMENTAL_ENTITIES: tuple[tuple[str, str], ...] = (
    ("Customer", "customers"),
    ("Invoice", "invoices"),
    ("Payment", "payments"),
    ("Item", "items"),
    ("Account", "accounts"),
    ("Vendor", "vendors"),
    ("Bill", "bills"),
    ("BillPayment", "bill_payments"),
    ("JournalEntry", "journal_entries"),
    ("CreditMemo", "credit_memos"),
    ("RefundReceipt", "refund_receipts"),
    ("Estimate", "estimates"),
    ("PurchaseOrder", "purchase_orders"),
    ("Purchase", "purchases"),
    ("Deposit", "deposits"),
    ("Transfer", "transfers"),
    ("TimeActivity", "time_activities"),
    ("Employee", "employees"),
)

# 6 replace-only resources (small static sets / singletons).
REPLACE_ENTITIES: tuple[tuple[str, str], ...] = (
    ("TaxAgency", "tax_agencies"),
    ("TaxRate", "tax_rates"),
    ("Class", "classes"),
    ("Department", "departments"),
    ("CompanyInfo", "company_info"),
    ("Preferences", "preferences"),
)
