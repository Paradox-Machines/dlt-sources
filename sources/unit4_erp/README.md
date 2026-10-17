# unit4_erp

Unit4 ERP dlt source. It loads finance and personnel objects from the **U4 ERPx
Object API** for one or more companies of a single tenant.

- **Source type:** 1, SaaS app API (CONNECTOR_SPEC §P1). **Level:** L2 on every
  resource. L3 history is not offered: the Object API has no change history.
- **Status:** alpha. It was built against the public ERPx reference and mocked
  tests only, and has not run against a live tenant. Unit4 documents the API for
  "U4 ERPx". Whether an **ERP CR** tenant (RMU) exposes the same Object API, and
  with which auth, is still to be confirmed. The base URL and both auth types are
  configuration for that reason.

API reference: <https://develop.unit4cloud.com/erpx/reference/object-api/>

## Configuration

```toml
# .dlt/secrets.toml
[sources.unit4_erp.credentials]
type = "oauth_client_credentials"          # or "basic"
token_url = "https://<ids-host>/connect/token"   # from the tenant's instance document
client_id = "..."
client_secret = "..."
# scope = "u4erp"                          # default
# basic: username = "...", password = "..."   (an ERP user)

# .dlt/config.toml
[sources.unit4_erp]
base_url = "https://<tenant-and-environment-api-root>"   # without /v1
company_ids = ["RM"]
```

Unit4 requires the M2M client's ERP user to hold the SUPER or SYSTEM role.
Each token is valid for one tenant only.

### Arguments

| Argument | Default | Meaning |
|---|---|---|
| `credentials` | secrets | See above. |
| `base_url` | config | Tenant and environment API root. Use Acceptance before Production. |
| `company_ids` | config | ERP `companyId` values. Each one becomes `_source_account_id`. |
| `start_date` | `None` (full history) | ISO date or timestamp. Floor for the first run of incremental objects. |
| `lookback` | 1 hour | Overlap subtracted from the cursor (§11.2). |
| `resources` | every unrestricted object + `field_definitions` | Resource names (table names). Restricted objects load only when named. |
| `delete_reconciliation_interval` | 1 day | How often incremental objects re-read their keys (§12.2 method 4). `None` turns it off. |
| `source_timezone` | `UTC` | IANA zone for timestamps sent without an offset. Filter literals are written in this zone. |
| `page_size` | 10,000 | Rows per call. Unit4 caps a call at 50,000. |
| `select` | `*.*` | `select` on data reads. `*.*` adds custom fields, contact points and related values. |
| `max_requests_per_minute` | 450 | Client-side ceiling below the tenant's 500/min. `0` turns it off. |
| `max_attempts` | 6 | Attempts per request on 429, 5xx and connection errors. |

```python
import dlt
from paradox_dlt_sources.unit4_erp import unit4_erp_source

pipeline = dlt.pipeline("unit4_erp", destination="duckdb", dataset_name="unit4_erp")
pipeline.run(unit4_erp_source())
```

## Resource matrix

There is one dlt resource per (object, company), named `{table}_{company}` (for
example `general_ledger_transactions_rm`). All of them write to the shared
`{table}`. Every resource is `merge` / `upsert`, and its primary key starts
with `_source_account_id`. The `companyId` key field equals
`_source_account_id`, so it is not repeated in the key. Nested key fields are
lifted to top-level columns: `additionalInformation/sequenceNumber` becomes
`additional_information__sequence_number`. History is "none" for every resource.

| Resource | Class | Level | Disposition | Key | Object API path | Change mechanism | Delete method |
|---|---|---|---|---|---|---|---|
| `general_ledger_transactions` | Entity | L2 | upsert | `_source_account_id, transactionNumber, additionalInformation__sequenceNumber` | `v2/objects/general-ledger-transactions` | cursor `lastUpdated/updatedAt` | key reconciliation, daily |
| `accounts` | Reference | L2 | upsert | `_source_account_id, account, periodFrom` | `v1/objects/accounts` | cursor | key reconciliation, daily |
| `account_groups` | Reference | L2 | upsert | `_source_account_id, accountGroup` | `v1/objects/account-groups` | cursor | key reconciliation, daily |
| `attributes` | Reference | L2 | upsert | `_source_account_id, attributeId, attributeName` | `v1/objects/attributes` | full read | key reconciliation, every run |
| `attribute_values` | Reference | L2 | upsert | `_source_account_id, attributeId, attributeValue, periodFrom` | `v1/objects/attribute-values` | cursor | key reconciliation, daily |
| `periods` | Reference | L2 | upsert | `_source_account_id, periodType, period` | `v1/objects/periods` | cursor | key reconciliation, daily |
| `companies` | Reference | L2 | upsert | `_source_account_id` | `v1/objects/companies` | cursor | key reconciliation, daily |
| `planner_transactions` | Entity | L2 | upsert | `_source_account_id, period, transactionId` | `v1/objects/planner-transactions` | cursor | key reconciliation, daily |
| `planner_versions` | Reference | L2 | upsert | `_source_account_id, version` | `v1/objects/planner-versions` | full read | key reconciliation, every run |
| `suppliers` | Entity | L2 | upsert | `_source_account_id, supplierId` | `v1/objects/suppliers` | cursor | key reconciliation, daily |
| `supplier_transactions` | Entity | L2 | upsert | `_source_account_id, transactionNumber, sequenceNumber` | `v2/objects/supplier-transactions` | cursor | key reconciliation, daily |
| `customers` | Entity | L2 | upsert | `_source_account_id, customerId` | `v1/objects/customers` | cursor | key reconciliation, daily |
| `customer_transactions` | Entity | L2 | upsert | `_source_account_id, transactionNumber, sequenceNo` | `v1/objects/customer-transactions` | cursor | key reconciliation, daily |
| `bank_statements` | Entity | L2 | upsert | `_source_account_id, statementId` | `v1/objects/bank-statements` | cursor | key reconciliation, daily |
| `bank_reconciliation_transactions` | Entity | L2 | upsert | `_source_account_id, reconciliationId, fixId, type, transactionNumber, sequenceNumber` | `v1/objects/bank-reconciliation-transactions` | cursor | key reconciliation, daily |
| `projects` | Entity | L2 | upsert | `_source_account_id, projectId` | `v1/objects/projects` | cursor | key reconciliation, daily |
| `work_orders` | Entity | L2 | upsert | `_source_account_id, workOrderId` | `v1/objects/work-orders` | cursor | key reconciliation, daily |
| `activities` | Entity | L2 | upsert | `_source_account_id, projectId, activityId` | `v1/objects/activities` | full read | key reconciliation, every run |
| `employees` | Entity | L2 | upsert | `_source_account_id, personId` | `v1/objects/employees` | cursor | key reconciliation, daily |
| `employments` | Entity | L2 | upsert | `_source_account_id, personId, employmentId, positionId` | `v1/objects/employments` | full read | key reconciliation, every run |
| `positions` | Entity | L2 | upsert | `_source_account_id, positionId` | `v1/objects/positions` | cursor | key reconciliation, daily |
| `position_codes` | Reference | L2 | upsert | `_source_account_id, positionCode` | `v1/objects/position-codes` | cursor | key reconciliation, daily |
| `resources` | Entity | L2 | upsert | `_source_account_id, personId` | `v1/objects/resources` | cursor | key reconciliation, daily |
| `payroll_transactions` *(restricted)* | Entity | L2 | upsert | `_source_account_id, batchId, transactionNumber, sequenceNumber` | `v1/objects/payroll-transactions` | cursor | key reconciliation, daily |
| `employee_rates` *(restricted)* | Entity | L2 | upsert | `_source_account_id, personId, positionId, valueReference, dateFrom` | `v1/objects/employee-rates` | cursor | key reconciliation, daily |
| `field_definitions` | Metadata | L2 | upsert | `_source_account_id, object_type, field_key` | schema of each selected object | full read | key reconciliation, every run |

The keys are the `x-u4-key` declared on each object's schema in the public
reference. `field_definitions` logs a warning when a tenant's schema serves
different keys.

### Foreign keys

| Column | Target |
|---|---|
| `*.accountingInformation.account`, `accounts.account` | `accounts.account` (by `periodFrom` validity) |
| `*.accountingInformation.accountingDimension1..7` | `attribute_values.attribute_value`. Which dimension is the cost center or fund is tenant setup: read it from `attributes` and the account rules. |
| `accounts.accountGroup` | `account_groups.account_group` |
| `*.period` | `periods.period` |
| `supplier_transactions.*supplierId*` | `suppliers.supplier_id` |
| `customer_transactions.customers` | `customers.customer_id` |
| `planner_transactions.budgetBookId`, `plannerVersion` | `planner_versions.version` |
| `activities.project_id`, `work_orders.projectId` | `projects.project_id` |
| `employments.person_id`, `employments.position_id` | `employees.person_id`, `positions.position_id` |
| `resources.person_id` | `employees.person_id` |
| `bank_reconciliation_transactions.transaction_number` | `general_ledger_transactions.transaction_number` |

The joining fields of nested references stay inside their `json` columns.

## Behaviour

### Paging

Paging is keyset, as Unit4 recommends ("optimised pagination"). Each call orders
on the key fields and filters past the last key read, for example
`(transactionNumber gt 1500048427) or (transactionNumber eq 1500048427 and
additionalInformation/sequenceNumber gt 2)`. A page shorter than `page_size`
ends the walk. A page longer than `page_size` fails the resource rather than
paging unreliably. Offset paging is never used: it slows down at high offsets
and skips rows when data changes mid-walk.

### Change capture (§11)

The Object API has no change feed. Extension Kit events go to Unit4 flows, not to
a feed an API client can consume. So:

- **Incremental objects** (those with `lastUpdated`) send the server-side filter
  `lastUpdated/updatedAt ge <cursor - lookback>`. The cursor is the highest
  `updatedAt` loaded by the previous run. It is written to resource state only
  after the walk, and dlt commits state only with a successful load.
- **Full-read objects** (`attributes`, `planner_versions`, `activities`,
  `employments`) have no `lastUpdated` and are read in full every run.

### Deletes (§12)

The API reports no deletes, so the source uses key reconciliation:

- After a full read, its keys are the live set.
- Every `delete_reconciliation_interval`, an incremental object re-reads only
  its keys (`select=companyId,transactionNumber,additionalInformation(sequenceNumber)`).
- Rows already in the destination whose key is not live are re-emitted
  **whole**, with `_is_deleted = true` and `_deleted_at`. Upserts replace the
  row, so a key-only tombstone would wipe its data. A key that comes back is
  upserted with `_is_deleted = false`.
- If the source returns no keys for a company that has rows loaded, the run
  fails and nothing is tombstoned. An empty read is more likely lost access
  than every record deleted.

Stored rows are read through the pipeline's SQL client, so the destination must
be SQL-queryable (duckdb, Postgres, Iceberg via the filesystem SQL client).

### Fields (§8)

- `select=*.*` by default, so custom fields (`customFieldGroups`), contact
  points and related values are loaded. The RMU lineage needs custom fields for
  IPEDS HR: race/ethnicity, tenure, rank and occupational category are not
  standard Object API properties.
- `max_table_nesting=0`: nested objects are kept as `json` columns.
- Standard columns: `_source_account_id`, `_source_updated_at`
  (`lastUpdated.updatedAt`), `_is_deleted`, `_deleted_at`, `_extracted_at`.
  `_source_created_at` is always NULL, because the Object API exposes no
  creation time.
- Money: amounts sit in `amounts` and `debitCreditFlag`, next to
  `currencyAmount` and the company currency. No conversion is applied.

### Derived columns

Nested key fields are copied to top-level columns
(`additional_information__sequence_number`). The original value stays in its
`json` column.

## Source limits

Limits per tenant, shared by every M2M client
(<https://develop.unit4cloud.com/erpx/limits-and-quotas/>):

- 500 requests per minute and 500,000 per day
- 50,000 rows per call, a 220 s call timeout, and a 750,000-row cap on one
  Object API retrieval
- 635 MB outbound per 5 minutes and 32 GB per day

A 429 returns `Retry-After`, and the source waits for it. 503 and other 5xx
errors are retried with backoff. Maintenance windows (Azure US) are on weekends
and Unit4 publishes them in Community4U. Schedule runs outside them. A
non-production environment that is not in use gets suspended, and its first call
can take up to 15 minutes while it wakes up.

## Exclusions

- **Every other Object API object** (528 list endpoints in the public reference)
  is excluded until RMU's tenant is confirmed. Coverage follows the RMU
  assessment: GL, chart of accounts and dimensions, budgets, AP/AR, bank
  reconciliation, projects and personnel. Adding an object is one entry in
  `settings.OBJECTS`.
- **Payroll** (`payroll_transactions`, `employee_rates`) is restricted under
  RMU's data classification (payroll is restricted). It loads only when named
  in `resources`.
- **Write endpoints, batch APIs and the Reports API** are out of domain.

## Source timezone and retention

- Timestamps carry an offset in the public examples (`Z`). A value without an
  offset is read in `source_timezone`, which defaults to UTC. Confirm the
  tenant's behaviour on the first live call.
- Raw responses are not archived (§16, optional for APIs).
