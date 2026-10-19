# hubspot

HubSpot dlt source — extracts CRM objects and engagements from
[HubSpot](https://hubspot.com) using the Private App bearer-token auth.

## Resources

| Resource | Primary key | Write disposition | Endpoint |
|---|---|---|---|
| `companies` | `id` | `append` (changes on `updatedAt`, deletes tombstoned) | GET `/crm/v3/objects/companies` |
| `contacts` | `id` | `append` (changes on `updatedAt`, deletes tombstoned) | GET `/crm/v3/objects/contacts` |
| `deals` | `id` | `append` (changes on `updatedAt`, deletes tombstoned) | GET `/crm/v3/objects/deals` |
| `engagements` | `id` | `append` (changes on `lastUpdated`, deletes tombstoned) | GET `/engagements/v1/engagements/paged` |
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
- **Change capture**: neither list endpoint filters by modification time,
  so every run walks the whole collection and lands only rows changed since
  the saved cursor minus `lookback_seconds` (default 1 hour). CRM v3 uses
  `updatedAt`; engagements v1 uses `lastUpdated` (integer milliseconds
  epoch). The saved cursor is capped at the moment the run started, so a
  record edited while the walk is in progress (after its page was read) is
  landed by the next run. The first run after upgrading adopts the old
  `dlt.sources.incremental` high-water mark, so nothing is reloaded.
  `deal_pipelines` is full-refresh (`replace`) — it is a small, static set.
- **Deletes**: because the walk is complete, a key that is live in the
  destination but missing from the walk was deleted or merged away in
  HubSpot. Its last landed row is appended again with `_is_deleted = true`
  and `_deleted_at` set; live rows carry `_is_deleted = false`. Read the
  newest row per `id` (by `_dlt_load_id`) and drop it when `_is_deleted`.
  A run that would tombstone more than `max_delete_share` (default 0.5) of a
  table's live keys raises instead, so a truncated read never wipes a table.
  Reconciliation starts on the second run after upgrading, once the
  `_is_deleted` column has landed.
- **Association IDs**: Engagement association ID arrays are serialized
  as JSON strings (`associations__contact_ids`, etc.) rather than dlt
  child tables, matching the `data->'associations'->'contactIds'` raw
  JSON shape the staging models read.
