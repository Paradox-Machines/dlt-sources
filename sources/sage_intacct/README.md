# Sage Intacct

dlt source for the [Sage Intacct REST API v1](https://developer.sage.com/intacct/).
Reads GL detail, AR / AP sub-ledgers, budgets and dimension tables through the
query service (`POST /services/core/query`). Multi-entity companies are read
entity by entity.

## Configuration

`.dlt/secrets.toml`:

```toml
[sources.sage_intacct]
company_id = "interstateac"
user_id = "ws_user"                 # Web Services user ID (case-sensitive)
client_id = "xxxx.app.sage.com"     # Sage developer-console application
client_secret = "..."
entity_ids = ["Interstate"]         # optional; omit or [] for the top level
```

| Key | Required | Default | Description |
|---|---|---|---|
| `company_id` | yes | — | Sage Intacct company ID. |
| `user_id` | yes | — | Web Services user the client application is authorised for. |
| `client_id` | yes | — | Client ID of the application in the Sage developer console. |
| `client_secret` | yes | — | Client secret of that application. |
| `entity_ids` | no | `[]` | Entity IDs to read. Empty reads the top level once. |
| `page_size` | no | `1000` | Records per query request (1–1000). |
| `initial_modified_since` | no | `2000-01-01T00:00:00Z` | `audit.modifiedDateTime` lower bound for the first incremental run. |
| `base_url` | no | `https://api.intacct.com/ia/api/v1` | REST API base URL. |

### Auth

OAuth 2.0 client credentials: `POST /oauth2/token` with `client_id`,
`client_secret` and `username`. The username is `user_id@company_id` at the top
level and `user_id@company_id|entity_id` for an entity. The client-credentials
grant does not take the Web Services user's password. Each entity gets its own
token. Tokens are cached and fetched again 5 minutes before `expires_in`
elapses, or after a 401.

### Sage Intacct setup

1. Create an application in the Sage developer console to get `client_id` /
   `client_secret`.
2. Create a Web Services user (**Company > Admin > Web Services users**).
3. Authorise the application for that user under **Company > Setup > Company >
   Security > Authorized client applications** (client ID + Web Services user
   ID).
4. For multi-entity companies, give the user access to every entity in
   `entity_ids` (unrestricted, or entity-level access per entity).

### Required Web Services permissions

Read (List + View) on:

| Module | Objects |
|---|---|
| Company | Entities, Locations, Departments, Classes, Employees |
| General Ledger | Accounts, Journal entries (incl. lines), Budgets |
| Accounts Receivable | Customers, Invoices, Payments |
| Accounts Payable | Vendors, Bills, Payments |
| Projects | Projects |

A missing permission surfaces as a `SageIntacctError` with the `ia::error`
code from the failing query.

## Resources

Primary key is `(entity_id, key)` on every table. Field lists live in
`settings.OBJECTS`; only those fields are requested and every one is declared
as a column hint, so zero-row syncs still create the full table.

| Resource | Sage object | Load |
|---|---|---|
| `entities` | `company-config/entity` | replace |
| `locations` | `company-config/location` | replace |
| `departments` | `company-config/department` | replace |
| `classes` | `company-config/class` | replace |
| `projects` | `projects/project` | incremental merge |
| `employees` | `company-config/employee` | incremental merge |
| `customers` | `accounts-receivable/customer` | incremental merge |
| `vendors` | `accounts-payable/vendor` | incremental merge |
| `gl_accounts` | `general-ledger/account` | incremental merge |
| `journal_entries` | `general-ledger/journal-entry` | incremental merge |
| `journal_entry_lines` | `general-ledger/journal-entry-line` | incremental merge |
| `ar_invoices` | `accounts-receivable/invoice` | incremental merge |
| `ar_invoice_lines` | `accounts-receivable/invoice-line` | incremental merge |
| `ar_payments` | `accounts-receivable/payment` | incremental merge |
| `ap_bills` | `accounts-payable/bill` | incremental merge |
| `ap_bill_lines` | `accounts-payable/bill-line` | incremental merge |
| `ap_payments` | `accounts-payable/payment` | incremental merge |
| `budgets` | `general-ledger/budget` | incremental merge |
| `budget_details` | `general-ledger/budget-detail` | incremental merge |

`journal_entry_lines` is the GL-detail table for P&L / gross margin / EBITDA:
it carries `txn_type` (debit/credit), `txn_amount`, `base_amount`,
`entry_date`, `gl_account__*`, `journal_entry__key` and the location /
department / employee / project / customer / vendor / class dimensions.

Column names are the dotted Sage field path, dlt-normalised and joined with
`__`: `audit.modifiedDateTime` → `audit__modified_date_time`,
`dimensions.department.id` → `dimensions__department__id`.

### Multi-entity

Every row carries `entity_id`: the configured entity it was read from, or `""`
for a top-level read. An entity-scoped read returns that entity's records, so
the same `key` can appear under several entities; the compound primary key
keeps them apart.

### Incremental

Incremental resources send `audit.modifiedDateTime >= cursor.start_value` and
merge on `(entity_id, key)`. `start_value` is the high-water mark from the
previous run — **not** `cursor.last_value`, which moves mid-extract and would
skip rows on later pages or entities. The cursor is shared across entities, so
each run re-reads from the highest timestamp seen in any entity.

## Usage

```python
import dlt
from paradox_dlt_sources.sage_intacct import sage_intacct_source

pipeline = dlt.pipeline(
    pipeline_name="sage_intacct", destination="duckdb", dataset_name="sage_intacct_data"
)
pipeline.run(sage_intacct_source(entity_ids=["Interstate"]))

# GL detail only
pipeline.run(sage_intacct_source().with_resources("journal_entry_lines", "journal_entries"))
```
