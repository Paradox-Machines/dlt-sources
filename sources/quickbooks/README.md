# quickbooks

QuickBooks Online dlt source — 24 resources via the QBO SQL-query layer,
matching Airbyte parity.

## Resources

| Resource | QBO Entity | Primary key | Write disposition |
|---|---|---|---|
| `customers` | `Customer` | `Id` | `merge` |
| `invoices` | `Invoice` | `Id` | `merge` |
| `payments` | `Payment` | `Id` | `merge` |
| `items` | `Item` | `Id` | `merge` |
| `accounts` | `Account` | `Id` | `merge` |
| `vendors` | `Vendor` | `Id` | `merge` |
| `bills` | `Bill` | `Id` | `merge` |
| `bill_payments` | `BillPayment` | `Id` | `merge` |
| `journal_entries` | `JournalEntry` | `Id` | `merge` |
| `credit_memos` | `CreditMemo` | `Id` | `merge` |
| `refund_receipts` | `RefundReceipt` | `Id` | `merge` |
| `estimates` | `Estimate` | `Id` | `merge` |
| `purchase_orders` | `PurchaseOrder` | `Id` | `merge` |
| `purchases` | `Purchase` | `Id` | `merge` |
| `deposits` | `Deposit` | `Id` | `merge` |
| `transfers` | `Transfer` | `Id` | `merge` |
| `time_activities` | `TimeActivity` | `Id` | `merge` |
| `employees` | `Employee` | `Id` | `merge` |
| `tax_agencies` | `TaxAgency` | `Id` | `replace` |
| `tax_rates` | `TaxRate` | `Id` | `replace` |
| `classes` | `Class` | `Id` | `replace` |
| `departments` | `Department` | `Id` | `replace` |
| `company_info` | `CompanyInfo` | `Id` | `replace` |
| `preferences` | `Preferences` | `Id` | `replace` |

## Auth flow — OAuth 2.0 with rotating refresh tokens

QuickBooks uses the standard OAuth 2.0 refresh-token grant, but with a
critical difference: **Intuit may rotate the refresh token on every token
exchange**. The old refresh token expires within approximately 24 hours.

Wire-level details:
- **Token endpoint**: `https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer`
- **Client credentials**: sent via HTTP Basic auth
  (`Authorization: Basic base64(client_id:client_secret)`), NOT in the
  form body. Placing credentials in the body yields `invalid_client`.
- **Request body**: `grant_type=refresh_token&refresh_token=<current_token>`
- **Response**: `access_token`, `expires_in`, and optionally a new
  `refresh_token`. If the response `refresh_token` differs from the one
  sent, rotation has occurred.

`RotatingRefreshTokenAuth` (in `helpers.py`) handles this automatically:
after each token exchange, if the response contains a new `refresh_token`
it calls `on_token_rotation(new_refresh_token)` and replaces the cached
token for subsequent requests.

## The `on_token_rotation` callback

```python
def on_token_rotation(new_refresh_token: str) -> None: ...
```

This optional parameter receives the **new** refresh token whenever Intuit
rotates it. **You must persist this token before the pipeline run ends** —
the previous token will stop working within ~24 hours.

What external users should do with it:

```python
import dlt
from paradox_dlt_sources.quickbooks import quickbooks_source

# Example: write to a file (replace with your secrets backend)
def save_token(new_token: str) -> None:
    with open(".quickbooks_refresh_token", "w") as f:
        f.write(new_token)

pipeline = dlt.pipeline(
    pipeline_name="quickbooks",
    destination="duckdb",
    dataset_name="quickbooks_data",
)
pipeline.run(
    quickbooks_source(
        client_id="...",
        client_secret="...",
        refresh_token="<current_refresh_token>",
        realm_id="<your_company_id>",
        on_token_rotation=save_token,
    )
)
```

If `on_token_rotation` is `None` (the default), a no-op is used internally.
This is safe for one-off runs where you know rotation will not occur, but
**for production use you should always wire a write-back callback**.

## Realm ID

The `realm_id` is Intuit's identifier for a QBO company. It appears:
- In the QBO URL as `/app/homepage?company=<realmId>`
- In Intuit Developer Portal under your connected app's authorized tenants
- In OAuth callback responses as `realmId`

Every API request targets `/v3/company/{realm_id}/query`. One source
instance maps to one company. To sync multiple companies, create one
pipeline per company with the appropriate `realm_id` and `refresh_token`.

## Config

`.dlt/secrets.toml`:

```toml
[sources.quickbooks]
client_id     = "your_intuit_client_id"
client_secret = "your_intuit_client_secret"
refresh_token = "your_current_refresh_token"
realm_id      = "your_qbo_company_id"
```

## Example

```python
import dlt
from paradox_dlt_sources.quickbooks import quickbooks_source

rotated_tokens: list[str] = []

pipeline = dlt.pipeline(
    pipeline_name="quickbooks_demo",
    destination="duckdb",
    dataset_name="quickbooks_data",
)
info = pipeline.run(
    quickbooks_source(
        on_token_rotation=rotated_tokens.append,
    )
)
print(info)

if rotated_tokens:
    print(f"New refresh token issued — persist it: {rotated_tokens[-1]}")
```

## Known limitations

- All QBO entities are fetched via the SQL-query layer
  (`/v3/company/{realmId}/query`). Attachments, reports, and batch
  operations are out of scope.
- Incremental cursor is `MetaData.LastUpdatedTime`. The 6 replace
  resources do not carry this field and are always fully reloaded.
- QBO hard-caps MAXRESULTS at 1000 rows per query page.
