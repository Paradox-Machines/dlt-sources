# servicetitan

ServiceTitan dlt source — loads **several ServiceTitan tenants** (one per
operating company) into one set of tables. Every row is tagged with
`tenant_id` + `tenant_name`, and every primary key is compound
`(tenant_id, id)`, because ServiceTitan ids are only unique within a tenant.

API reference: <https://developer.servicetitan.io/apis/>

## Resources

One dlt resource per (endpoint, tenant), named `{endpoint}_{tenant_id}` (e.g.
`jobs_1234567`), writing to the shared `{endpoint}` table. All tables use
`write_disposition="merge"` on `(tenant_id, id)`.

| Table | Module | Endpoint (under `/{module}/v2/tenant/{tenant}/`) | Pagination | Incremental | Scope on the ServiceTitan app |
|---|---|---|---|---|---|
| `jobs` | jpm | `export/jobs` | export token | continuation token | Job Planning & Management: Jobs (read) |
| `appointments` | jpm | `export/appointments` | export token | continuation token | Job Planning & Management: Appointments (read) |
| `projects` | jpm | `export/projects` | export token | continuation token | Job Planning & Management: Projects (read) |
| `job_cancel_reasons` | jpm | `export/job-cancel-reasons` | export token | continuation token | Job Planning & Management: Job Cancel Reasons (read) |
| `job_types` | jpm | `job-types?active=Any` | page / hasMore | `modifiedOnOrAfter` | Job Planning & Management: Job Types (read) |
| `customers` | crm | `export/customers` | export token | continuation token | CRM: Customers (read) |
| `locations` | crm | `export/locations` | export token | continuation token | CRM: Locations (read) |
| `contacts` | crm | `export/customers/contacts` | export token | continuation token | CRM: Customers (read) |
| `bookings` | crm | `export/bookings` | export token | continuation token | CRM: Bookings (read) |
| `invoices` (+ `invoices__items`) | accounting | `export/invoices` | export token | continuation token | Accounting: Invoices (read) |
| `payments` | accounting | `export/payments` | export token | continuation token | Accounting: Payments (read) |
| `inventory_bills` (AP bills) | accounting | `export/inventory-bills` | export token | continuation token | Accounting: Inventory Bills (read) |
| `purchase_orders` (+ `purchase_orders__items`) | inventory | `export/purchase-orders` | export token | continuation token | Inventory: Purchase Orders (read) |
| `vendors` | inventory | `vendors?active=Any` | page / hasMore | `modifiedOnOrAfter` | Inventory: Vendors (read) |
| `estimates` (+ `estimates__items`) | sales | `export/estimates` | export token | continuation token | Sales & Estimates: Estimates (read) |
| `business_units` | settings | `export/business-units` | export token | continuation token | Settings: Business Units (read) |
| `technicians` | settings | `export/technicians` | export token | continuation token | Settings: Technicians (read) |
| `employees` | settings | `export/employees` | export token | continuation token | Settings: Employees (read) |
| `form_submissions` | forms | `submissions` | page / hasMore | none (full read each run) | Forms: Submissions (read) |

The table lives in `settings.ENDPOINTS`; adding an endpoint is one entry.

Column notes:

- `technicians` declares `team`, `business_unit_id`, `main_zone_id`,
  `zone_ids`, `role_ids` and `skills` up front (lists land as JSON).
- `form_submissions.units` holds the whole filled-in form tree (refrigerant
  usage, parts, unit details) as one JSON column, so arbitrary form layouts
  never explode into per-section child tables. `owners` is JSON too.
- `invoices`, `purchase_orders` and `estimates` keep their line `items` in a
  `<table>__items` child table, joined on `_dlt_parent_id`.
- Source-level `max_table_nesting=2`; anything deeper lands as JSON.

### Incremental behaviour

Each (endpoint, tenant) resource keeps its own state, so a quiet tenant never
inherits a busier tenant's high-water mark.

- **Export endpoints** walk `continueFrom` tokens. The last token is saved in
  resource state and sent as `from` on the next run, which then receives only
  records changed since. `start_date` (`YYYY-MM-DD`) seeds the first run as
  `from=<date>`; without it the first run exports full history.
- **Page endpoints with `modifiedOnOrAfter`** are incremental on `modifiedOn`.
  The filter is `cursor.start_value` — the stable high-water mark from the
  *previous* run — **not** `cursor.last_value`, which shifts mid-extract and
  would silently drop rows (the repo-wide convention). `start_date` seeds the
  first run.
- `form_submissions` has no server-side modified filter wired, so it re-reads
  all submissions each run and merges.

### Errors and rate limits

- **403** on one endpoint for one tenant (the app lacks that scope there) logs
  a warning naming the tenant, resource and missing scope, and skips just that
  resource for that tenant. Everything else still loads.
- **429** waits for the `Retry-After` header (capped at 120 s) and retries;
  429/5xx/connection errors retry up to `max_attempts` (default 8).
- Any other HTTP error fails the run.

## Auth

OAuth 2.0 **client credentials** against
`https://auth.servicetitan.io/connect/token`, with a per-tenant `client_id` /
`client_secret`. The bearer token is cached per tenant until 60 s before
expiry. Every API call also sends the `ST-App-Key` header; one app key is
shared by all tenants.

Setup in the ServiceTitan developer portal:

1. Create one app and enable the read scopes in the table above.
2. Install the app on each tenant; each install issues a `client_id` and
   `client_secret`.
3. Note the app key and each tenant id.

## Config

`.dlt/secrets.toml`:

```toml
[sources.servicetitan]
app_key = "ak1.your-app-key"

[[sources.servicetitan.tenants]]
tenant_id = "1234567"
name = "Interstate"
client_id = "cid.interstate"
client_secret = "cs1.interstate"

[[sources.servicetitan.tenants]]
tenant_id = "7654321"
name = "OSC"
client_id = "cid.osc"
client_secret = "cs1.osc"
```

| Key | Required | Default | Notes |
|---|---|---|---|
| `tenants` | yes | — | List of `{tenant_id, name, client_id, client_secret}` |
| `app_key` | yes | — | `ST-App-Key`, shared by every tenant |
| `start_date` | no | `None` | `YYYY-MM-DD` floor for the first run |
| `resources` | no | all | Endpoint names to load, e.g. `["jobs", "invoices"]` |
| `page_size` | no | `500` | `pageSize` for page endpoints |
| `max_attempts` | no | `8` | Attempts per request on 429 / 5xx |
| `api_base_url` | no | `https://api.servicetitan.io` | Also `SERVICETITAN_API_BASE_URL` |
| `auth_base_url` | no | `https://auth.servicetitan.io` | Also `SERVICETITAN_AUTH_BASE_URL` |

Use `https://api-integration.servicetitan.io` /
`https://auth-integration.servicetitan.io` for the integration environment.

## Example

```python
import dlt
from paradox_dlt_sources.servicetitan import servicetitan_source

pipeline = dlt.pipeline(
    pipeline_name="servicetitan",
    destination="duckdb",
    dataset_name="servicetitan_data",
)
info = pipeline.run(servicetitan_source(start_date="2024-01-01"))
print(info)
```

Only some endpoints:

```python
pipeline.run(servicetitan_source(resources=["jobs", "invoices", "technicians"]))
```

## Known limitations

- Built against the public API docs without live credentials. Endpoint paths
  and query params are in `settings.ENDPOINTS`; verify them against a live
  tenant before the first production run.
- Export endpoints are not sent `includeRecentChanges`; very recent edits can
  arrive one run later.
- Hard deletes are not detected. Inactive records arrive with `active = false`
  where the endpoint reports them.
