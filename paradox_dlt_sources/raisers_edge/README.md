# raisers_edge

Blackbaud Raiser's Edge NXT, read through the [SKY API](https://developer.blackbaud.com/skyapi).

- **Source type:** 1, SaaS app API (spec §P1).
- **Level:** L2 (change capture and deletes). No resource reaches L3: SKY API keeps no
  field history for these records, and `<entity>_versions` (§13.2) is not built yet.

## Credentials

```toml
[sources.raisers_edge]
client_id = "..."          # SKY application ID
client_secret = "..."      # SKY application secret
refresh_token = "..."      # from the one-time consent below
subscription_key = "..."   # Bb-Api-Subscription-Key, from My subscriptions
source_timezone = "America/New_York"   # the RE environment's time zone, required
```

SKY API supports only the OAuth 2.0 authorization code flow. There is no
client-credentials grant. Setup:

1. Register an application at developer.blackbaud.com. Add a redirect URI.
2. An environment admin with Marketplace rights connects it: Marketplace → Manage →
   Connect app, then paste the application ID.
3. A user consents once at
   `https://app.blackbaud.com/oauth/authorize?client_id=...&response_type=code&redirect_uri=...`.
   Exchange the code (valid 5 minutes) at `https://oauth2.sky.blackbaud.com/token` with
   `grant_type=authorization_code`. The response's `refresh_token` is the secret.

The connector reads with **that user's RE permissions**. A user who cannot see gifts or a
custom field category gets no rows for them. Use an account with read access to every
record type.

The access token lasts 60 minutes and refreshes inside the run. The refresh token lasts
365 days:

- `preserve_refresh_token=True` (default) keeps one value until it expires. Consent again
  before day 365.
- `preserve_refresh_token=False` rotates the token on every refresh with a sliding expiry.
  Pass `on_token_rotation` and persist every new value. Losing one breaks the connection.

## Arguments

| Argument | Default | Meaning |
|---|---|---|
| `source_timezone` | required | Zone for naive SKY date-times (§8.2). |
| `start_date` | everything | First incremental read for each cursor resource. |
| `lookback` | 1 hour | Overlap on each cursor read. |
| `resources` | all | Subset to load. |
| `delete_reconciliation_interval` | 1 day | Key reconciliation cadence. `None` turns off delete detection. |
| `max_delete_share` | 0.5 | One run refuses to tombstone more than this share of a table's live keys. |
| `preserve_refresh_token` | `True` | See Credentials. |
| `on_token_rotation` | no-op | Called with each rotated refresh token. |

`include_history` and `include_versions` (§14) are not accepted yet, because neither
history nor versions exist at L2.

## Resource matrix

All tables are `merge` / `upsert` and carry the §10.1 columns.

| Resource | Class | Level | Key | Change mechanism | Delete method | FKs |
|---|---|---|---|---|---|---|
| constituents | entity | L2 | id | cursor `last_modified` | key reconciliation | – |
| constituent_addresses | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_email_addresses | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_phones | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_online_presences | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_relationships | entity | L2 | id | cursor | key reconciliation | constituent_id, relation_id → constituents; reciprocal_relationship_id → constituent_relationships |
| constituent_educations | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_codes | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| constituent_custom_fields | entity | L2 | id | cursor | key reconciliation | parent_id → constituents; category → field_definitions (object_type `constituent`) |
| constituent_notes | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| actions | entity | L2 | id | cursor | key reconciliation | constituent_id, opportunity_id, campaign_id, fund_id, appeal_id |
| memberships | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents |
| gifts | entity | L2 | id | cursor | key reconciliation | constituent_id → constituents; batch_number → gift_batches.batch_number |
| gift_splits | child | L2 | (gift_id, id) | parent `gifts` | `_parent_version` | gift_id → gifts; fund_id, campaign_id, appeal_id, package_id |
| gift_soft_credits | child | L2 | (gift_id, id) | parent `gifts` | `_parent_version` | gift_id → gifts; constituent_id → constituents |
| gift_custom_fields | entity | L2 | id | cursor | key reconciliation | parent_id → gifts; category → field_definitions (`gift`) |
| campaigns | entity | L2 | id | cursor | key reconciliation | – |
| funds | entity | L2 | id | cursor | key reconciliation | – |
| appeals | entity | L2 | id | cursor | key reconciliation | – |
| packages | entity | L2 | id | cursor | key reconciliation | appeal_id → appeals |
| opportunities | entity | L2 | id | cursor | key reconciliation | constituent_id, campaign_id, fund_id, appeal_id |
| events | entity | L2 | id | cursor (offset paging) | key reconciliation | – |
| event_participants | entity | L2 | id | cursor, per event | key reconciliation | event_id → events; constituent_id → constituents |
| gift_batches | reference | L2 | id | full read each run | key reconciliation each run | – |
| lists | reference | L2 | id | full read each run | key reconciliation each run | – |
| field_definitions | metadata | L2 | (object_type, field_key) | full read each run | key reconciliation each run | – |
| field_options | metadata | L2 | (object_type, field_key, value) | full read each run | key reconciliation each run | field_key → code table name |
| currency_configuration | reference | L2 | currency_code | full read each run | key reconciliation each run | – |

### Why a cursor, not a change feed (§11.1)

SKY API's only change feed is webhooks
(`com.blackbaud.constituent.delete.v1`, `com.blackbaud.gift.change.v1`, …). Webhooks
need a public HTTPS receiver, which a batch connector cannot provide. They also cover
only constituents, their child records, gifts and opportunities, with nothing for funds,
campaigns, appeals, events, notes, education or memberships. Every list-all endpoint
supports a server-side `last_modified` filter with `sort_token` paging, so the cursor is
server-side (§P1).

- The filter is inclusive (`on or after`).
- Each read starts at the saved cursor minus `lookback`.
- A key seen twice in one read changed mid-read. The later copy is dropped, and the cursor
  is held at its timestamp, so the next run picks it up.
- SKY lists come from Blackbaud's read-optimized store, refreshed about every 30 minutes.
  `date_modified` there is the store's refresh time, not the time of the user's edit.
  Running more often than every 30 minutes adds calls and no data.
- A parent's `date_modified` does not change when its children change. That is why every
  child collection (addresses, emails, phones, …) is read from its own list-all endpoint.

### Deletes (§12.2 method 4)

List endpoints never return deleted records. On the reconciliation interval each entity
resource does a full read (`last_modified=0001-01-01`, which still gets `sort_token`).
Every live destination key missing from that read becomes a tombstone: the last loaded
row with `_is_deleted = true` and `_deleted_at` set to the detection time. A key that
reappears is restored. Known keys come from the destination (`pipeline.dataset()`).

`include_inactive=true` and `include_deceased=true` are always sent. Without them SKY
hides inactive and deceased records, and reconciliation would tombstone them.

`gift_splits` and `gift_soft_credits` arrive inside the gift. Each run re-emits a changed
gift's full set, stamped `_parent_version` (the gift's `_source_updated_at`). The
current set is the rows whose `_parent_version` equals the gift's current version.

## Fields and types

- **All fields.** List responses are loaded whole. New fields land without code changes.
- **Objects** are flattened one level: `amount__value`, `email__address`, `address__city`,
  `birthdate__y`. **Lists** (`gift_splits`, `payments`, `acknowledgements`, `receipts`,
  `fundraisers`, …) stay `json` on the parent row. Splits and soft credits also get their
  own tables.
- **Custom fields** are rows (`constituent_custom_fields`, `gift_custom_fields`) keyed by
  `category`, not columns. `field_definitions` lists each category per record type.
- **Code tables** (`field_options`): RE stores the entry's description on records, so
  `value` and `label` are both the description.
- **Timestamps:** `date_added` and `date_modified` carry a UTC offset and load as UTC.
  Other naive date-times are read in `source_timezone`.
- **Date-only fields** come as midnight date-times and load as `date`: gift `date` and
  `post_date`, action `date`, address `start`/`end`, membership `joined`/`expires`, and
  custom field `date`.
- **Money:** `*__value` columns are decimal. RE environments hold one currency, so the
  derived `currency_code` (from `/constituent/v1/currencyconfiguration`) sits beside them
  on gifts, splits, soft credits and memberships.

Derived columns: `currency_code`, `event_id` on participants, `list_type` on lists,
`gift_id` and `_parent_version` on gift children, and every §10 column.

## Excluded

| Data | Reason |
|---|---|
| Aliases, ratings, prospect status, fundraiser assignments, giving summaries, solicit codes | Per constituent only. About 80K constituents means 80K calls per table, beyond the Standard quota. |
| Membership transactions, benefits, cards | Per membership only. |
| Event fees, expenses, participant fees/payments/donations | Per event or per participant only. Candidates for a later per-event pass. |
| Attachments, profile pictures | Binary documents, out of domain. |
| Consents, communication preferences | Different paging (`continuation_token`, `from_date`). Next to add. |
| SSN / tax ID | Not exposed by SKY API. |
| Email marketing engagement (sends, opens, clicks) | No SKY API exists for it. |
| Query API, Volunteer, Gift Aid (UK), Standard Reports | Not needed for L2 coverage; Gift Aid is UK-only. |

## Source limits

- **Rate:** 10 calls per second. A 429 includes `Retry-After`, and dlt's session honours it.
- **Quota:** Free tier 1,000 calls a day; Standard 25,000, which must be claimed manually.
  A spent quota returns 403 "Out of call volume quota" and the run fails with
  `SkyQuotaExceededError`. Quotas reset at 00:00 UTC.
- **Page size:** 5,000 rows per call (events and participants allow 10,000).
- **Call cost:** one call per 5,000 rows per table, plus one call per event per run for
  `event_participants`. That walk is the most expensive part of a frequent schedule.

## Raw retention

None. The API can be re-read (§16).
