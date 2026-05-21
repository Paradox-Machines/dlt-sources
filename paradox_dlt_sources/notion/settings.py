"""Notion source — endpoint constants and API configuration."""

from __future__ import annotations

# Base URL — override via NOTION_API_BASE_URL env var (set in __init__.py)
NOTION_API_BASE_URL = "https://api.notion.com"

# Notion requires a dated API version header on every request.
# Pinned to the same release the legacy Airbyte connector used so that
# downstream dbt models get the same field surface.
NOTION_API_VERSION = "2022-06-28"

# Incremental cursor starting point — the Unix epoch expressed as an ISO-8601
# timestamp with Zulu offset.  Notion's `last_edited_time` fields are always
# RFC 3339 strings so lexicographic comparison works correctly.
EPOCH_ISO = "1970-01-01T00:00:00Z"

# Notion API path prefixes
PATH_USERS = "/v1/users"
PATH_SEARCH = "/v1/search"
PATH_BLOCKS_CHILDREN = "/v1/blocks/{page_id}/children"
PATH_COMMENTS = "/v1/comments"

# HTTP status codes with special handling
HTTP_NOT_FOUND = 404
