"""apollo_io source — endpoint constants and resource configuration."""

from __future__ import annotations

APOLLO_IO_API_BASE_URL: str = "https://api.apollo.io/v1"

ENDPOINT_CONTACTS_SEARCH: str = "/contacts/search"
ENDPOINT_ACCOUNTS_SEARCH: str = "/accounts/search"
ENDPOINT_PEOPLE_SEARCH: str = "/mixed_people/search"
ENDPOINT_OPPORTUNITIES: str = "/opportunities/search"
ENDPOINT_SEQUENCES: str = "/emailer_campaigns/search"
ENDPOINT_USERS: str = "/users/search"
ENDPOINT_EMAIL_ACCOUNTS: str = "/email_accounts"
ENDPOINT_LABELS: str = "/labels"

DEFAULT_PER_PAGE: int = 100

RESOURCES: tuple[str, ...] = (
    "contacts",
    "accounts",
    "people",
    "opportunities",
    "sequences",
    "users",
    "email_accounts",
    "labels",
)
