# loxo

Loxo dlt source — extracts people, jobs, companies, deals, activities, and
users from the [Loxo](https://loxo.com) ATS / recruiting CRM API.

## Resources

| Resource | Primary key | Write disposition | Endpoint |
|---|---|---|---|
| `people` | `id` | `replace` | GET `/people` |
| `jobs` | `id` | `replace` | GET `/jobs` |
| `companies` | `id` | `replace` | GET `/companies` |
| `deals` | `id` | `replace` | GET `/deals` |
| `activities` | `id` | `replace` | GET `/person_events` |
| `users` | `id` | `replace` | GET `/users` |

> Note: the `activities` resource hits Loxo's `/person_events` endpoint —
> Loxo's URL naming differs from the friendlier dlt resource name. A request
> to `/activities` returns 404.

## Auth

Bearer API token via `Authorization: Bearer <api_key>`. Obtain from your
Loxo account under API / Integrations. The base URL is per-agency:

```
https://{domain}/api/{agency_slug}
```

- `domain` defaults to `app.loxo.co` (every SaaS agency on Loxo's hosted plan).
- `agency_slug` is per-customer.
- `LOXO_API_BASE_URL` env var overrides the full base URL for the rare
  custom-domain agency: e.g. `https://recruiting.example.com/api/acme`.

## Config

`.dlt/secrets.toml`:

```toml
[sources.loxo]
agency_slug = "your-agency-slug"
api_key = "your_loxo_api_key"
```

`domain` is optional and defaults to `app.loxo.co`. Set it in
`.dlt/config.toml` if you serve from a custom Loxo subdomain.

## Example

```python
import dlt
from paradox_dlt_sources.loxo import loxo_source

pipeline = dlt.pipeline(
    pipeline_name="loxo_demo",
    destination="duckdb",
    dataset_name="loxo_data",
)
info = pipeline.run(loxo_source())
print(info)
```

## Pagination

Loxo's pagination is mixed:

- **Scroll cursor** — `/people`, `/companies`, `/deals`, `/person_events`.
  The response includes a `scroll_id` opaque token; pass it back as a query
  param on the next call. Terminal when the field is absent or the page
  is empty. Implemented in `LoxoScrollIdPaginator`.
- **Page number** — `/jobs` only. Standard `?page=N&per_page=100`.
- **Single page** — `/users` returns a single response with no pagination
  metadata documented.

`/companies` and `/deals` reject the `per_page` query param with HTTP 422,
so the scroll paginator omits it entirely and relies on Loxo's server-side
default page size.

## Known limitations

- **No documented `updated_at` filter.** Listing endpoints don't appear to
  support a `since=` / `updated_at_from=` parameter, so every resource uses
  `write_disposition="replace"` (full snapshot per run). If a filter does
  exist in practice, the source can be promoted to `append` +
  `dlt.sources.incremental("updated_at", ...)` — see the module docstring
  for the path.
- **`scroll_id` stability across runs is unverified.** If Loxo's cursor is
  session-bound, it cannot be persisted across runs. This currently doesn't
  matter (every resource is `replace`) but gates future incremental work.
- **Endpoint quirks**: `/companies` and `/deals` reject `per_page` (422);
  `/activities` does not exist (use `/person_events`). The source has guard
  tests for both.
