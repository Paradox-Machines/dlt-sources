# attio

Attio dlt source — extracts CRM records, lists, and notes from
[Attio](https://attio.com).

## Resources

| Resource | Primary key | Write disposition | Notes |
|---|---|---|---|
| `companies` | `record_id` | `replace` | POST `/v2/objects/companies/records/query` |
| `people` | `record_id` | `replace` | POST `/v2/objects/people/records/query` |
| `deals` | `record_id` | `replace` | POST `/v2/objects/deals/records/query` |
| `lists` | `list_id` | `replace` | GET `/v2/lists` (returns all in single response) |
| `notes` | `note_id` | `replace` | GET `/v2/notes` (single-page until cursor confirmed) |

## Auth

API key as `Authorization: Bearer <key>`. Mint at Attio → Workspace
settings → Developers → API tokens. Required scopes:

- `companies`/`people`/`deals`: `record_permission:read`, `object_configuration:read`
- `lists`: `list_configuration:read`
- `notes`: `note:read`, `object_configuration:read`, `record_permission:read`

Missing scope → 403 → resource skipped with a `WARNING` log; other
resources continue.

## Config

`.dlt/secrets.toml`:

```toml
[sources.attio]
api_key = "your_attio_api_key"
```

## Example

```python
import dlt
from paradox_dlt_sources.attio import attio_source

pipeline = dlt.pipeline(
    pipeline_name="attio_demo",
    destination="duckdb",
    dataset_name="attio_data",
)
info = pipeline.run(attio_source())
print(info)
```

To extract custom objects beyond the defaults:

```python
attio_source(objects=("companies", "people", "deals", "my_custom_object"))
```

## Known limitations

- Records: `replace` mode (full re-snapshot per run). Attio records expose
  `created_at` but no record-level `updated_at`, so no incremental cursor
  is possible until Attio adds one.
- Notes pagination: `/v2/notes` cursor shape is undocumented. Currently
  uses `SinglePagePaginator` (returns the first page only). Confirm shape
  before enabling multi-page extraction.
- Lists pagination: `/v2/lists` returns ALL lists in one response and
  ignores offset/limit — `SinglePagePaginator` is the correct choice
  (OffsetPaginator would loop forever).
