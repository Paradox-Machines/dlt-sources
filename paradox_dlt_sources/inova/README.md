# iNova Payroll (UKG Ready) source

Loads workforce and payroll data for an iNova Payroll tenant into any dlt
destination: legal entities, cost centers (departments), employees,
compensation history, payroll runs, and per-employee pay statements with
earnings, deductions and taxes (employee + employer amounts).

## Research: which product is this?

| Finding | Source |
|---|---|
| Vendor confirmed (2026-10-15): Interstate AC's ServiceTitan tenant has an installed API app "Inova Payroll", published by Inova Payroll (https://www.inovapayroll.com). The product is Inova Payroll. | Client ServiceTitan tenant (PA-1032) |
| iNova Payroll (Nashville, inovapayroll.com) sells "Inova HCM", a private-labelled **UKG Ready** — it has been UKG Ready's largest reseller since 2007. UKG agreed to acquire Inova in Dec 2025; customers stay on UKG Ready. | [UKG newsroom](https://www.ukg.com/company/newsroom/ukg-agrees-acquire-inova-payroll), [Inova news](https://inovapayroll.com/inova-news/ukg-acquisition-agreement/) |
| Inova advertises a free "Open API" REST API for the HCM suite but publishes no endpoint reference of its own. | [Inova Open API](https://inovapayroll.com/partners/open-api/), [Inova FAQs](https://inovapayroll.com/resources/faqs/) |
| Inova's login page also lists **Evolution** (Asure's payroll platform) for legacy clients. Inova is discontinuing Evolution; this source does **not** support Evolution. | [Inova log-in](https://inovapayroll.com/resources/log-in/), [Evolution notice](https://www.evolutionpayrollservices.com/inova-payroll-discontinues-evolution-payroll-software) |
| It is **not** isolved People Cloud. isolved's REST reference (`myisolved.com/rest`) is login-gated and unrelated. | — |

**Conclusion:** the API is the public UKG Ready REST API (v2), documented at the
[UKG Ready Developer Hub](https://secure.saashr.com/ta/docs/rest/public/).
All endpoints below come from that reference. Nothing was verified against a
live tenant — credentials were not available.

## API summary

| Topic | Detail | Doc |
|---|---|---|
| Base URL | `https://{pod}/ta/rest/v2/companies/{cid}` — `pod` is the tenant host (iNova white-label hosts also serve `/ta/`); `cid` is the numeric company id, unique id, or `\|short_name` / `!short_name` | [Foundations](https://secure.saashr.com/ta/docs/rest/public/#/foundations) |
| Auth | OAuth 2.0 `client_credentials`: `POST /ta/rest/v2/companies/{cid}/oauth2/token`, form body `grant_type`, `client_id`, `client_secret` → `access_token` (`expires_in` 3600). Then `Authorization: Bearer <token>`. Client is created as a Machine-to-Machine OAuth app in Company Setup. | [Client id and secret](https://secure.saashr.com/ta/docs/rest/public/#/client-id-and-secret), [oauth2/token](https://secure.saashr.com/ta/docs/rest/public/#/resources) |
| Pagination | None on the collection endpoints used here — each returns the full list (`employees`, `compensations`, `items`, `payrolls`, `pay_statements`, `cost_centers`). Large responses can hit the service limit instead. | endpoint pages |
| Incremental | `GET /employees/changed?since=<ISO datetime>` (max 31 days back) returns `entries[]` with `status` `CREATED` / `MODIFIED` / `DELETED`. `GET /payrolls` filters by `pay_date_from` / `pay_date_to` (range must be < 1 year). No change filter on compensation, EINs or cost centers. | `employees/changed`, `payrolls` |
| Rate limits | Rolling 60 s window by active employees: 0–249 → 500 calls, 250–499 → 750, 500–999 → 1000, 1000+ → 1500. Throttled calls return **HTTP 503** with `X-CallLimit-TimeToWait` (ms); without the header wait ≥ 60 s. Response-size service limit → **HTTP 413**. | [Limits](https://secure.saashr.com/ta/docs/rest/public/#/limits), [HTTP headers](https://secure.saashr.com/ta/docs/rest/public/#/http-headers) |

### Endpoints

| Need | Endpoint | Used |
|---|---|---|
| Companies (legal entities) | `GET /lookup/eins` → `items[]` (`id`, `ein_name`, `ein_tax_id`, `ein_currency_code`, `ein_country_id`) | `companies` |
| Departments / org levels | `GET /config/cost-centers?tree_index=0..8` → `cost_centers[]` (`id`, `parent_id`, `name`, `payroll_code`, `external_id`, `location_code`) | `cost_centers` |
| Employees | `GET /employees` (`ein_id`, `terminated` filters) → `employees[]` incl. `ein_name`, `dates{hired,re_hired,started,terminated}`, `status` | `employees` |
| Employment status changes | `GET /employees/changed?since=` | `employees` (incremental) |
| Employee detail | `GET /employees/{aid}` (cost centers, termination details, **also SSN / DOB**) | not used |
| Pay rates / compensation history | `GET /compensation/history` → `compensations[]` (`effective_from`, `amount`, `amount_period`, `hourly_pay`, `currency`, `employee.account_id`); per-employee `GET /employees/{aid}/compensation/history` | `compensation_history` |
| Payroll runs | `GET /payrolls` → `payrolls[]` (`pay_date`, `start_date`, `end_date`, `type`, `ein{id}`, `status`) | `payrolls` |
| Pay statements | `GET /payrolls/{id}/pay-statements` then `GET /payrolls/{id}/pay-statements/{id}` → `earnings[]` (`hours`, `base_rate`, `ee_amount`, `earning{code}`, `cost_centers[]`), `deductions[]` (`ee_amount`, `er_amount`), `taxes[]` (`ee_amount`, `er_amount`, wage bases, `company_tax{code}`), `payments[]` | `pay_statements` + line tables |
| Time / labour | `GET /time-entries`, `GET /employees/{aid}/time-entries`, `/employees/{aid}/timesheets` | not yet — follow-up |
| Bank / direct deposit | `/employees/{aid}/direct-deposits` | never called |

## Resources

| Table | Disposition | Key | Notes |
|---|---|---|---|
| `companies` | replace | `id` | EINs; `ein_id` = `id` |
| `cost_centers` | replace | `tree_index`, `id` | trees from `cost_center_tree_indexes` (default `(0,)`) |
| `employees` | merge | `id` | full list first run; `/employees/changed` after; `_deleted`, `_change_status`, `_synced_at` |
| `compensation_history` | replace | `id` | `account_id` + `ein_id` via employee → EIN |
| `payrolls` | merge | `id` | incremental on `pay_date`, re-reads `payroll_lookback_days` (31) |
| `pay_statements` | merge | `id` | one detail call per statement; `payments`/`messages` as child tables |
| `pay_statement_earnings` / `_deductions` / `_taxes` | merge | `id` | `payroll_id`, `pay_statement_id`, `account_id`, `pay_date` |

Every row has `company_id` (the UKG `cid`) and `ein_id` (legal entity;
`NULL` on `cost_centers`, which are tenant-wide). Interstate AC's five operating
companies are expected to be five EINs under one `cid` — confirm with the client.

Incremental cursors pass `cursor.start_value` (never `last_value`) to the API.

## PII

- `strip_pii` removes `social_security`, `ssn`, `national_id_numbers`,
  `birthday`, `pin`, bank/direct-deposit keys and `_links` from every row.
- `GET /employees/{aid}` and bank endpoints are not called.
- Errors carry status code and path only; rows are never logged.

## Configuration

```toml
# .dlt/config.toml
[sources.inova]
company_id = "123456"        # or "|SHORTNAME"
pod = "secure.saashr.com"    # tenant host — confirm with iNova

# .dlt/secrets.toml
[sources.inova]
client_id = "..."
client_secret = "..."
```

## Open questions (need client credentials)

1. Exact pod host and `cid` for Interstate AC; confirm the tenant is UKG Ready, not Evolution.
2. Confirm the five operating companies are EINs (vs. a cost-center tree).
3. Which cost-center tree index holds departments, and how COGS vs OpEx is coded
   (cost center, earning code, or GL mapping).
4. Whether `object` in `/employees/changed` matches the `/employees` list shape.
5. Statement-detail call volume vs. the 60 s call limit on large back-fills.
