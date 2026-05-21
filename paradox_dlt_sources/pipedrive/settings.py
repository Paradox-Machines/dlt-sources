"""Pipedrive source — endpoint constants, resource names, and auth settings."""

from __future__ import annotations

# Pipedrive v1 API base URL. Override via PIPEDRIVE_API_BASE_URL env var
# (or pass `base_url=` kwarg) for testing / sandbox environments.
PIPEDRIVE_API_BASE_URL = "https://api.pipedrive.com/v1"

# Default page size for all list endpoints. Pipedrive v1 max is 500;
# 100 keeps response payloads manageable and matches legacy Airbyte defaults.
DEFAULT_PAGE_SIZE = 100

# Default start offset for all list endpoints.
DEFAULT_START = 0

# Resources (object names) that this source extracts.
RESOURCES: tuple[str, ...] = (
    "users",
    "persons",
    "leads",
    "organizations",
    "deals",
    "activities",
    "stages",
)

# Pipedrive's `/v1/activities` endpoint filters by the API-token user by
# default — every other resource returns all-company records when `user_id`
# is omitted. `user_id=0` means "all company users."
# See Pipedrive v1 docs: https://developers.pipedrive.com/docs/api/v1/Activities#getActivities
ACTIVITIES_ALL_USERS_PARAM: dict[str, int] = {"user_id": 0}

# Cursor floor for ISO-timestamp incrementals. Real watermarks are always
# strictly greater than this; use as the "load everything" initial value.
EPOCH_ISO = "1970-01-01T00:00:00Z"
