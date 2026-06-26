"""monday_crm source — endpoint constants and resource configuration."""

from __future__ import annotations

MONDAY_CRM_API_BASE_URL: str = "https://api.monday.com/v2"

QUERY_BOARDS: str = (
    "query ($limit: Int, $page: Int) { "
    "boards(limit: $limit, page: $page) { "
    "id name description state board_kind workspace_id "
    "columns { id title type } "
    "groups { id title } "
    "} }"
)

QUERY_ITEMS: str = (
    "query ($boardId: ID!, $limit: Int, $cursor: String) { "
    "boards(ids: [$boardId]) { "
    "items_page(limit: $limit, cursor: $cursor) { "
    "cursor "
    "items { id name state board_id "
    "group { id title } "
    "column_values { id text value column { id title type } } "
    "created_at updated_at "
    "} } } }"
)

QUERY_USERS: str = (
    "query ($limit: Int, $page: Int) { "
    "users(limit: $limit, page: $page) { "
    "id name email enabled created_at "
    "teams { id name } "
    "} }"
)

QUERY_TEAMS: str = "query { teams { id name picture_url users { id name email } } }"

QUERY_TAGS: str = "query { tags { id name color } }"

QUERY_UPDATES: str = (
    "query ($limit: Int, $page: Int) { "
    "updates(limit: $limit, page: $page) { "
    "id body text_body created_at updated_at item_id "
    "replies { id body text_body created_at creator { id name } } "
    "creator { id name } "
    "} }"
)

QUERY_WORKSPACES: str = (
    "query ($limit: Int, $page: Int) { "
    "workspaces(limit: $limit, page: $page) { "
    "id name kind description "
    "} }"
)

RESOURCES: tuple[str, ...] = (
    "boards",
    "items",
    "users",
    "teams",
    "tags",
    "updates",
    "workspaces",
    "columns",
    "groups",
)
