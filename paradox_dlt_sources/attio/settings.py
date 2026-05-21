"""Attio source — endpoint constants and default object slugs."""

from __future__ import annotations

ATTIO_API_BASE_URL = "https://api.attio.com"

# Standard objects we pull records for. Attio also has `users` and
# `workspaces` standard objects, plus user-defined custom objects;
# override via the `objects=` kwarg on the source factory.
STANDARD_OBJECTS: tuple[str, ...] = ("companies", "people", "deals")

# OAuth scopes documented per resource. Used in the 403-skip warning.
SCOPES_RECORDS = "record_permission:read, object_configuration:read"
SCOPES_LISTS = "list_configuration:read"
SCOPES_NOTES = "note:read, object_configuration:read, record_permission:read"
