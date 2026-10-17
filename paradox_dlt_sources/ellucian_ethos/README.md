# ellucian_ethos

Ellucian Banner through **Ellucian Ethos Integration** (Integration Management).
It reads Ethos Data Model resources (`persons`, `sections`,
`section-registrations`, …) over HTTPS with an Ethos application API key. It
needs no database access, so it works for Banner SaaS.

Source type: SaaS app API (CONNECTOR_SPEC §P1). Level: **L2 for every resource**
(L3 versions where the change feed is on; see below).

## Credentials

```toml
[sources.ellucian_ethos.credentials]
api_key = "…"   # API key of the Ethos application this connector runs as
```

`POST /auth` exchanges the key for a JWT that expires after 5 minutes. The source
refreshes it 60 s before expiry, so long backfills are fine.

### What RMU's Ethos administrator sets up

1. An Ethos application for Paradox in the **Test** environment first, then Production.
2. **Data access**: Banner as the authoritative source for every resource in the matrix,
   with proxy credentials for a read-only Banner API user. Use `BAN_DEFAULT_Q` security
   objects, so writes return 405.
3. **Subscriptions** on the same application for every resource in the matrix, created
   **before the first run**. They are what makes the change feed work. Without them, run with
   `change_feed=False`.
4. Optional: GORDMSK masking for fields Paradox must not receive. Masked fields are dropped
   and logged (`X-Content-Restricted`).

## Arguments

| Argument | Default | Meaning |
|---|---|---|
| `credentials` | secrets | `api_key` |
| `base_url` | `https://integrate.elluciancloud.com` | Other regions: `.ca`, `.ie`, `.com.au` |
| `resources` | every unrestricted resource | Ethos names (`section-registrations`). Restricted ones load only when named |
| `extra_resources` | none | More Ethos resources, such as RMU custom APIs |
| `versions` | latest | Pin per resource: `{"persons": "12"}` or `"16.0.0"`. **Pin after the first live run**, so a Banner upgrade cannot change a resource's shape |
| `change_feed` | `True` | Consume change notifications. `False`: a full read every run |
| `resync` | `False` | A full read of everything this run |
| `include_versions` | `True` | `<table>_versions` from the change feed |
| `delete_reconciliation_interval` | 1 day | How often feed-driven resources are read in full to find deletes. `None` relies on the feed's `deleted` operations |
| `page_size` | 500 | Lowered automatically to `X-Max-Page-Size` |
| `feed_batch_size` | 1000 | Messages per `/consume` call (one call per run) |
| `start_date`, `lookback`, `include_history` | — | Not applicable (no modification-time filter, no field history). Accepted for the standard interface and ignored |

## How changes, deletes and history are captured

Ethos has no `updated_since` filter that works across resources. The change mechanism is
therefore Ethos' **change notifications** (§11.1 option 1). Banner publishes one for every
create, update and delete on a subscribed resource.

| Situation | What a run does |
|---|---|
| First run of a resource, `resync=True`, or the reconciliation interval has passed | Full `offset`/`limit` read. If the walk is complete, ids missing from it are tombstoned (§12.2 method 4) |
| Other runs | One `/consume` batch. The latest message per id is upserted, and `deleted` becomes a tombstone that keeps the row's data |
| `change_feed=False` | Full read plus reconciliation, every run (§11.1 option 3) |

- **Acknowledgement is safe.** `/consume` is a queue: passing `lastProcessedID` removes
  the messages up to it. The source passes only the position committed by the last
  *successful* load, and reads one batch per run. A failed run therefore re-reads the same
  messages. When `X-Remaining` says more are queued, it logs a warning; schedule runs often
  enough, or raise `feed_batch_size`.
- **During a full read the feed is not applied to that resource's table.** The full
  read is already current. The messages still go to `_versions` and `change_notifications`.
- **Incomplete walks never delete.** Offset paging over changing data can skip rows.
  If a walk returns fewer rows than its first `X-Total-Count`, reconciliation is skipped
  with a warning. An empty read against a loaded table raises.
- **Notifications without content** (a subscription option) are completed by fetching
  `GET /api/{resource}/{id}` in the message's version. A 404 means the record is deleted.
- **Versions (§13.2)**: `<table>_versions` has one row per published change, keyed
  `(id, _change_position)`. Resolution is every change after go-live. The state before the
  first run comes from the entity table's first load and from the DW and Patriot archives.
  Census snapshots are built from this table.
- **Source-native history (§13.1)**: none. Ethos serves current state only.

## Resource matrix

Every resource has the same shape:

| Class | Disposition | Key | Change mechanism | Delete method | History |
|---|---|---|---|---|---|
| Entity / reference | `merge` / `upsert` | `id` (Ethos GUID) | change feed, else full read | feed `deleted`, plus key reconciliation | `<table>_versions` (feed) |

| Group | Resources (Banner tables) |
|---|---|
| People | `persons` (SPRIDEN, SPBPERS, GORPRAC), `addresses` (SPRADDR), `person-visas` (GORVISA), `person-external-education` (SORHSCH, SORPCOL), and the types: `races`, `ethnicities`, `gender-identities`, `citizenship-statuses`, `countries`, `regions`, `address-types`, `email-types`, `phone-types`, `person-name-types`, `alternative-credential-types` |
| Academic structure | `academic-periods` (STVTERM, SOBPTRM), `academic-levels`, `academic-programs` (SMRPRLE, SOBCURR), `academic-disciplines` (STVMAJR), `academic-credentials`, `cip-codes`, `educational-institution-units` (STVCOLL, STVDEPT, STVDIVS), `educational-institutions`, `subjects`, `courses` (SCBCRSE), `sections` (SSBSECT), `instructional-methods`, `instructional-delivery-methods`, `instructors` (SIBINST) |
| Student | `students` (SGBSTDN), `student-types`, `residency-types`, `student-academic-programs` (SORLCUR), `student-academic-periods` (SFBETRM), `student-academic-period-profiles`, `student-academic-credentials` (degrees), `student-academic-standings` (SHRTTRM), `academic-standings`, `student-grade-point-averages`, `student-transcript-grades`, `student-cohorts`, `student-cohort-assignments` (SGRCHRT), `section-registrations` (SFRSTCR), `section-registration-statuses`, `campus-involvements` (SGRSPRT, SGRSACT), `campus-organizations`, `student-aptitude-assessments` (SORTEST), `aptitude-assessments` |
| Admissions | `admission-applications` (SARADAP), `admission-decisions` (SARAPPD), `admission-decision-types`, `admission-application-types`, `admission-application-status-types`, `admission-populations` |
| Financial aid | `student-financial-aid-awards` and `restricted-student-financial-aid-awards` (RPRAWRD; both are needed), `financial-aid-funds` (RFRBASE), `financial-aid-fund-categories`, `financial-aid-fund-classifications`, `financial-aid-years`, `financial-aid-award-periods` |
| Restricted (load only when named) | `financial-aid-applications`, `financial-aid-application-outcomes` (RCRAPP1: FAFSA income and family data) |
| Student accounts | `student-charges`, `student-payments` (TBRACCD), `accounting-codes` (TBBDETC) |

Plus `change_notifications`: an event table, `insert-only` on the message `id`. It holds
every consumed message as received, including those for resources this run did not load.
Once consumed, a message is gone from Ethos, so this table is the replay log.

**Foreign keys** are Ethos GUID references inside the `json` columns, such as
`section_registrations.registrant.id` → `persons.id`, `section_registrations.section.id`
→ `sections.id`, `sections.course.id` → `courses.id`, and `students.person.id` → `persons.id`.
Every referenced type resource is in the matrix, so they resolve within the dataset.

## Columns

- Raw Ethos properties keep their names. Nested objects and arrays are `json`
  (`max_table_nesting=0`).
- Standard columns (§10): `_source_created_at` and `_source_updated_at` come from EEDM
  `metadata.createdOn` / `modifiedOn` when Banner sends them, otherwise the notification's
  `published` time on feed rows. Also `_is_deleted`, `_deleted_at` and `_extracted_at`.
- `_ethos_media_type`: the representation version a row was served in (`X-Media-Type`,
  or the notification's `resource.version`).
- Versions tables add `_change_op` (`insert`, `update`, `delete`), `_change_position`
  (the message id, zero-padded) and `_committed_at` (`published`).
- Source timezone: Ethos datetimes carry an offset. Any that do not are read as UTC.

## Excluded, and source limits

- **Tuition and fee rates (SFRRGFE)**: no Ethos resource. `student-charges` has charges
  per student, not rate tables.
- **`field_definitions` (§7.3)**: Ethos publishes resource JSON schemas only in
  Ellucian's API catalog, behind a customer login. Not loaded yet.
- **Banner internal keys**: PIDM is not exposed. The Banner ID is in `persons.credentials`.
- **Rate limits**: none documented. 429 and 5xx retry with backoff and honour `Retry-After`.
- **QAPI** (`POST /qapi/{resource}`) and **GraphQL / Data Access** are not used. QAPI is
  unpaged, and Data Access needs Banner to load data there first.
- **Raw retention (§16)**: not archived. The source can be re-read, and
  `change_notifications` keeps every consumed message.

## Data classification

`persons` carries date of birth, gender, races and ethnicity. It may also carry SSN in
`credentials`, unless GORDMSK masks it. `change_notifications.content` holds the same
data for any resource. Tag these columns Restricted on landing. FAFSA resources are
restricted by default.

## Not yet verified against a live tenant

From Ellucian's documentation, not yet seen in RMU's tenant: `X-Max-Page-Size`,
`X-Remaining`, the `/consume` parameters (`limit`, `lastProcessedID`), the
change-notifications `v2` media type, and which Banner table backs
`student-academic-credentials`. Check them on the first Test-environment run.
