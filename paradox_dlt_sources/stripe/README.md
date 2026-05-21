# stripe

Stripe dlt source — extracts charges, customers, invoices, and refunds from
[Stripe](https://stripe.com).

## Resources

| Resource | Primary key | Write disposition | Incremental cursor |
|---|---|---|---|
| `charges` | `id` | `append` | `created` (epoch int → timestamp) |
| `customers` | `id` | `append` | `created` (epoch int → timestamp) |
| `invoices` | `id` | `append` | `created` (epoch int → timestamp) |
| `refunds` | `id` | `append` | `created` (epoch int → timestamp) |

All four resources use `created[gt]=<cursor>` for server-side incremental
filtering.  `range_start="open"` ensures the boundary record is not
double-loaded on the next run.

## Auth

API key as `Authorization: Bearer <sk_...>`.  Mint a restricted key at
Stripe → Developers → API keys.  Read-only access to the four resource
types above is sufficient.

## Config

`.dlt/secrets.toml`:

```toml
[sources.stripe]
api_key = "sk_live_..."
```

## Example

```python
import dlt
from paradox_dlt_sources.stripe import stripe_source

pipeline = dlt.pipeline(
    pipeline_name="stripe_demo",
    destination="duckdb",
    dataset_name="stripe_data",
)
info = pipeline.run(stripe_source())
print(info)
```

## Known behaviour

- **Pagination**: Stripe uses `has_more` + `starting_after=<last_id>` (cursor
  on the last item's `id`).  Pages are fetched in ascending `created` order
  (Stripe default).
- **Invoices — subscription hoist**: Stripe deprecated the top-level
  `subscription` field on Invoice objects; the id now lives at
  `parent.subscription_details.subscription`.  The source hoists it back to
  top-level so dbt staging models that reference `subscription` continue to
  work without changes.
- **`max_table_nesting=1`**: Applied to all resources to prevent deeply nested
  arrays (e.g. `invoices.lines`) from generating table names longer than
  Postgres's 63-character identifier limit.
