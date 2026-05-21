# agree_com

Agree.com dlt source — extracts agreements, contacts, and invoices from
[Agree.com](https://agree.com).

## Resources

| Resource | Primary key | Write disposition | Endpoint |
|---|---|---|---|
| `agreements` | `id` | `replace` | GET `/api/v1/agreements` |
| `contacts` | `id` | `append` | GET `/api/v1/contacts` |
| `invoices` | `id` | `replace` | GET `/api/v1/invoices` |

## Auth

API key as `Authorization: Bearer <key>`. Obtain from your Agree.com account
settings under API / Integrations.

## Config

`.dlt/secrets.toml`:

```toml
[sources.agree_com]
api_key = "your_agree_com_api_key"
```

## Example

```python
import dlt
from paradox_dlt_sources.agree_com import agree_com_source

pipeline = dlt.pipeline(
    pipeline_name="agree_com_demo",
    destination="duckdb",
    dataset_name="agree_com_data",
)
info = pipeline.run(agree_com_source())
print(info)
```

## Known limitations

- `agreements` and `invoices` use `replace` disposition (full re-snapshot per
  run). Neither resource exposes a monotonic last-modified timestamp:
  `agreements.starts_at` / `agreements.executed_at` fire once at
  creation/execution and do not advance when, e.g., new signatures arrive;
  `invoices.inserted_at` is similarly immutable. A true incremental cursor
  requires Agree.com to add an `updated_at` field to these endpoints.
- `contacts` uses an `updated_at` incremental cursor (`append` disposition).
  This relies on the Agree.com API returning contacts filtered or ordered by
  `updated_at`; if the API does not honour the cursor server-side, all contacts
  are fetched on every run and dlt deduplicates via the primary key.
- Pagination uses `PageNumberPaginator` against `pagination.total_pages`. The
  Agree.com OpenAPI docs at `https://secure.agree.com/documentation/openapi`
  are sometimes stale; the pagination shape has been verified against the live
  API.
