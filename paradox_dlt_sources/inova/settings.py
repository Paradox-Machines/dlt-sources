"""iNova (UKG Ready) source — endpoint constants and resource configuration."""

from __future__ import annotations

# Base URL template. `pod` is the tenant's UKG Ready host (white-labelled for
# iNova); `cid` is the company id or `|short_name` / `!short_name`.
BASE_URL_TEMPLATE = "https://{pod}/ta/rest/v2/companies/{cid}"

ENDPOINT_TOKEN = "/oauth2/token"
ENDPOINT_EINS = "/lookup/eins"
ENDPOINT_COST_CENTERS = "/config/cost-centers"
ENDPOINT_EMPLOYEES = "/employees"
ENDPOINT_EMPLOYEES_CHANGED = "/employees/changed"
ENDPOINT_COMPENSATION_HISTORY = "/compensation/history"
ENDPOINT_PAYROLLS = "/payrolls"
ENDPOINT_PAY_STATEMENTS = "/payrolls/{payroll_id}/pay-statements"
ENDPOINT_PAY_STATEMENT = "/payrolls/{payroll_id}/pay-statements/{statement_id}"

RESOURCES: tuple[str, ...] = (
    "companies",
    "cost_centers",
    "employees",
    "compensation_history",
    "payrolls",
    "pay_statements",
)

# `/employees/changed?since=` rejects values more than 31 days in the past.
# One day of margin keeps a slow run from crossing the limit mid-request.
CHANGED_MAX_LOOKBACK_DAYS = 30

# `/payrolls` requires the pay-date range to be shorter than one year.
PAYROLL_WINDOW_DAYS = 364
PAYROLL_DEFAULT_HISTORY_DAYS = 364

# Cost-center trees are indexed 0..8; which tree holds departments is
# tenant configuration.
COST_CENTER_TREE_INDEXES: tuple[int, ...] = (0,)
COST_CENTER_MAX_TREE_INDEX = 8

# Throttling: HTTP 503 + `X-CallLimit-TimeToWait` (ms). Docs: wait >= 60s
# when the header is absent.
THROTTLE_STATUS = 503
THROTTLE_WAIT_HEADER = "X-CallLimit-TimeToWait"
THROTTLE_DEFAULT_WAIT_SECONDS = 60.0
THROTTLE_MAX_RETRIES = 5
SERVICE_LIMIT_STATUS = 413

TOKEN_REFRESH_SAFETY_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 60

# Keys stripped (recursively) from every row before it leaves the source.
# Covers national identifiers (SSN etc.), DOB, PINs and bank details.
PII_EXCLUDED_KEYS: frozenset[str] = frozenset(
    {
        "ssn",
        "social_security",
        "social_security_number",
        "social_insurance",
        "national_insurance",
        "national_id_numbers",
        "bsn",
        "birthday",
        "birth_date",
        "date_of_birth",
        "pin",
        "bank_account",
        "bank_accounts",
        "account_number",
        "routing_number",
        "direct_deposit",
        "direct_deposits",
    }
)
