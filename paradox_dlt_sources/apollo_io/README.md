# apollo_io

# apollo_io

Apollo.io dlt source — loads CRM and prospecting data from the [Apollo.io REST API](https://apolloio.github.io/apollo-api-docs/).

## Resources

| Resource        | Write disposition | Incremental | Primary key |
|-----------------|-------------------|-------------|-------------|
| contacts        | append            | ✅ updated_at | id         |
| accounts        | replace           | ❌           | id          |
| people          | replace           | ❌           | id          |
| opportunities   | replace           | ❌           | id          |
| sequences       | replace           | ❌           | id          |
| users           | replace           | ❌           | id          |
| email_accounts  | replace           | ❌           | id          |
| labels          | replace           | ❌           | id          |

## Auth

Apollo.io authenticates via an API key sent in the `X-Api-Key` HTTP header.

Store your key in `secrets.toml`:

```toml
[sources.apollo_io]
api_key = "your-apollo-api-key"
```

## Config

| Parameter  | Default                        | Description                  |
|------------|-------------------------------|------------------------------|
| `api_key`  | `dlt.secrets.value`            | Apollo.io API key (required) |
| `base_url` | `https://api.apollo.io/v1`     | Override for testing only    |

## Example

```python
import dlt
from paradox_dlt_sources.apollo_io import apollo_io_source

pipeline = dlt.pipeline(
    pipeline_name="apollo_io",
    destination="duckdb",
    dataset_name="apollo_io_data",
)

source = apollo_io_source(api_key="your-apollo-api-key")
pipeline.run(source)
```

## Pagination

- **POST search endpoints** (contacts, accounts, people, opportunities, sequences, users): manual page-number loop using `page` + `per_page` in the POST body. Stops when an empty array is returned.
- **GET endpoints** (email_accounts, labels): `ApolloIoPagePaginator` injects `?page=N&per_page=100` query params and stops on an empty response.

## Known limitations

- **Incremental contacts**: Apollo does not expose a native `updated_at[gt]` filter. The connector approximates incremental behavior by sorting descending on `contact_updated_at` and stopping when rows are older than the last cursor value. Some rows near the boundary may be re-ingested.
- **People search costs credits**: each call to `/mixed_people/search` consumes Apollo API credits. Use this resource sparingly.
- **Rate limits**: plan-dependent; the connector does not currently implement automatic back-off.
- **API version**: all endpoints use `/v1/`. Newer Apollo v2 endpoints are not covered.

