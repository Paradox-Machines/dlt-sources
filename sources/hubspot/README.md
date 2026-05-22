# hubspot

HubSpot dlt source — extracts CRM objects and engagements from
[HubSpot](https://hubspot.com) using the Private App bearer-token auth.

## Resources

| Resource | Primary key | Write disposition | Endpoint |
|---|---|---|---|
| `companies` | `id` | `append` (incremental on `updatedAt`) | GET `/crm/v3/objects/companies` |
| `contacts` | `id` | `append` (incremental on `updatedAt`) | GET `/crm/v3/objects/contacts` |
| `deals` | `id` | `append` (incremental on `updatedAt`) | GET `/crm/v3/objects/deals` |
| `engagements` | `id` | `append` (incremental on `lastUpdated`) | GET `/engagements/v1/engagements/paged` |
| `deal_pipelines` | `id` | `replace` | GET `/crm/v3/pipelines/deals` |

## Auth

API key as `Authorization: Bearer <token>`.  Mint a Private App token at
HubSpot → Settings → Integrations → Private Apps.  The token is
long-lived; no OAuth refresh is required.

## Config

`.dlt/secrets.toml`:

```toml
[sources.hubspot]
api_key = "your_hubspot_private_app_token"
```

## Example

```python
import dlt
from paradox_dlt_sources.hubspot import hubspot_source

pipeline = dlt.pipeline(
    pipeline_name="hubspot_demo",
    destination="duckdb",
    dataset_name="hubspot_data",
)
info = pipeline.run(hubspot_source())
print(info)
```

## Known limitations

- **CRM v3 properties**: Only the fields listed in `settings.py` are
  returned.  HubSpot ignores unlisted properties — add any new staging
  model columns to the appropriate `*_PROPERTIES` tuple and redeploy.
- **Engagements v1**: CRM v3 split engagements into separate per-type
  objects (calls, emails, meetings, notes, tasks).  The v1 endpoint is
  used to preserve the unified shape (`type` discriminator column) the
  staging models rely on.  HubSpot has not yet sunset v1.
- **Incremental state**: CRM v3 uses `updatedAt`; engagements v1 uses
  `lastUpdated` (integer milliseconds epoch).  `deal_pipelines` is
  full-refresh (`replace`) — it is a small, static set.
- **Association IDs**: Engagement association ID arrays are serialized
  as JSON strings (`associations__contact_ids`, etc.) rather than dlt
  child tables, matching the `data->'associations'->'contactIds'` raw
  JSON shape the staging models read.
