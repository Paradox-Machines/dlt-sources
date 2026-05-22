# notion

Notion dlt source — extracts workspace members, databases, pages, blocks, and
comments from [Notion](https://www.notion.so) using a Notion internal
integration token.

## Resources

| Resource | Primary key | Write disposition | Endpoint |
|---|---|---|---|
| `users` | `id` | `replace` | `GET /v1/users` |
| `databases` | `id` | `append` (incremental) | `POST /v1/search` (filter=database) |
| `pages` | `id` | `append` (incremental) | `POST /v1/search` (filter=page) |
| `blocks` | `id` | `append` (incremental) | `GET /v1/blocks/{page_id}/children` |
| `comments` | `id` | `append` | `GET /v1/comments?block_id=<id>` |

### Incremental behaviour

`databases`, `pages`, and `blocks` use `last_edited_time` as the incremental
cursor. Results from the search endpoints are returned **newest first**, so the
extractor stops paging as soon as it encounters an item older than the prior
high-water mark — avoiding full re-scans of large workspaces.

`blocks` are **not** sorted by the API, so stale blocks are *skipped
individually* rather than triggering an early break.

`comments` have no monotonic update timestamp; the resource appends all
comments for each ingested block and relies on dedup-by-`id` at the staging
layer.

## Auth

Notion uses **internal integration tokens** (Bearer auth).  Mint one at
Notion → Settings → Integrations → New integration.  The integration must be
shared with each page/database you want ingested (Share → Add integration in
the Notion UI).

Required capabilities:
- `Read content` — needed for all resources
- `Read comments` — needed for the `comments` resource

## Config

`.dlt/secrets.toml`:

```toml
[sources.notion]
integration_token = "secret_..."
```

## Example

```python
import dlt
from paradox_dlt_sources.notion import notion_source

pipeline = dlt.pipeline(
    pipeline_name="notion_demo",
    destination="duckdb",
    dataset_name="notion_data",
)
info = pipeline.run(notion_source())
print(info)
```

To run only specific resources:

```python
source = notion_source()
pipeline.run([source.users, source.databases])
```

## Known limitations and edge cases

- **Unshared pages / databases** — pages not shared with the integration return
  HTTP 404 from the blocks endpoint. The extractor logs a `WARNING` and skips
  them; other pages continue normally. Check the log output for
  `"not shared with page"` messages if blocks seem missing.

- **Archived databases** — Notion's `POST /v1/search` does **not** return
  archived databases by default. Archived items disappear from the search
  results, so they will not appear in the `databases` resource. This matches
  Airbyte's behaviour.

- **Deleted pages** — similarly invisible to `/v1/search`; once deleted, a
  page falls out of incremental syncs naturally.

- **`comments` capability** — if the integration was minted without the
  `Read comments` capability, Notion returns HTTP 400
  (`unauthorized_capability`). The extractor re-raises this as an error
  (unlike 404s which are skipped) so operators see an actionable message.
  Re-mint the integration token with the capability enabled to resolve it.

- **Rate limiting** — Notion imposes a rate limit of roughly 3 requests/second
  per integration. For workspaces with thousands of pages the `blocks`
  transformer (one request per page) may trigger 429 responses. dlt's
  `RESTClient` does not auto-retry 429s by default; add a custom retry policy
  or reduce parallelism if you hit limits.

- **`properties` as JSON** — page `properties` are stored as an opaque JSON
  string column because Notion property key names are workspace-specific and
  differ across BUs. Use DuckDB's `json_extract` / `json_extract_string` in
  dbt to parse them.

- **Block content fields** — each block type (paragraph, heading, to_do, …)
  puts its content under a type-specific key (e.g. `paragraph.rich_text`).
  The `blocks` resource stores the raw object; dbt unpacks the content fields
  by type. The `type` column is always present to enable type-based routing.
