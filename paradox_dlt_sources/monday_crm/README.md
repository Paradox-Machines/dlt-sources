# monday_crm

# monday_crm

Extract boards, items, users, teams, tags, updates, workspaces, columns, and
groups from the monday.com GraphQL API (monday Sales CRM).

## Resources

| Resource   | Write disposition | Incremental | Primary key |
|------------|------------------|-------------|-------------|
| boards     | replace          | —           | id          |
| items      | merge            | updated_at  | id          |
| users      | replace          | —           | id          |
| teams      | replace          | —           | id          |
| tags       | replace          | —           | id          |
| updates    | replace          | —           | id          |
| workspaces | replace          | —           | id          |
| columns    | replace          | —           | id          |
| groups     | replace          | —           | id          |

## Auth

Personal API token or OAuth2 access token — passed as a Bearer token in the
`Authorization` header. Store it in `secrets.toml`:

```toml
[sources.monday_crm]
api_key = "eyJhbGciOiJIUzI1NiJ9..."
```

## Config

No additional configuration is required. The `base_url` defaults to
`https://api.monday.com/v2` and should not normally need overriding.

## Example

```python
import dlt
from paradox_dlt_sources.monday_crm import monday_crm_source

pipeline = dlt.pipeline(
    pipeline_name="monday_crm",
    destination="duckdb",
    dataset_name="monday_crm_data",
)

source = monday_crm_source(api_key="<your-token>")
info = pipeline.run(source)
print(info)
```

## Pagination

- **boards, users, updates, workspaces**: page-number pagination
  (`page=` integer variable in GraphQL variables). Stops when the returned
  list is shorter than the page size.
- **items**: cursor-based pagination per board via `items_page`. The cursor
  is read from `data.boards[0].items_page.cursor` and injected into the next
  request. Stops when the cursor is absent or null.
- **teams, tags, columns, groups**: single-page (no pagination).

## Known limitations

- No server-side `updated_since` filter — incremental filtering on `updated_at`
  is done client-side. All items are fetched and dlt applies the cursor filter.
- The `items_page` cursor may only be valid for the duration of a single
  paginated session; it should not be reused across independent sync runs as a
  bookmark.
- monday CRM objects (contacts, leads, deals) are modelled as board items —
  there are no dedicated CRM-specific endpoints.
- Rate limits (per-minute / per-day query complexity) are not documented in the
  research artifact.
