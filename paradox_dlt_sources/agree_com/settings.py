"""Agree.com source — endpoint constants and resource configuration."""

from __future__ import annotations

AGREE_API_BASE_URL = "https://secure.agree.com"

# Resources emitted by this source.
RESOURCES: tuple[str, ...] = ("agreements", "contacts", "invoices")

# Agree.com v1 REST endpoints for each resource.
ENDPOINT_AGREEMENTS = "/api/v1/agreements"
ENDPOINT_CONTACTS = "/api/v1/contacts"
ENDPOINT_INVOICES = "/api/v1/invoices"

# Default page size for paginated requests.
DEFAULT_PAGE_SIZE = 100
