# Changelog

All notable changes to `paradoxmachines-dlt-sources` are documented here.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
and the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

## [Unreleased]

### Added
- `apollo_io` — contacts (incremental), accounts, people, opportunities, sequences, users, email_accounts, labels. X-Api-Key header auth. Page-number pagination.
- `apollo_io` — contacts (incremental), accounts, people, opportunities, sequences, users, email_accounts, labels. X-Api-Key header auth. Page-number pagination.
- `monday_crm` — boards, items (incremental on updated_at), users, teams, tags, updates, workspaces, columns, groups. Bearer-token auth. GraphQL POST endpoint with page-number and cursor-based pagination.
- `monday_crm` — boards, items (incremental on updated_at), users, teams, tags, updates, workspaces. Bearer-token auth. GraphQL POST endpoint with page-number and cursor-based pagination.
- `monday_crm` — boards, items (incremental on updated_at), users, teams, tags, updates, workspaces. Bearer-token auth. GraphQL POST endpoint with page-number and cursor-based pagination.

## [0.1.0a7] — 2026-06-11

### Added
- `stripe`: `invoice_line_items` resource — a transformer over `invoices` that yields each invoice's embedded `lines.data[]` (stamped with `invoice_id`) and fetches `/v1/invoices/{id}/lines` for invoices whose lines overflow the embedded page. No own cursor; incrementality inherited from `invoices` (PAR-365).

## [0.1.0a2] — 2026-05-21

### Added
- `agree_com` — agreements, contacts (incremental), invoices, schedules.
- `stripe` — charges (cursor pagination), customers, invoices, refunds. Epoch→ISO timestamp coercion; subscription-id hoist on legacy invoice shape.
- `hubspot` — companies, contacts, deals, engagements (v1 offset paginator), deal_pipelines. Private-app bearer token auth.
- `pipedrive` — persons, deals, leads, activities, organizations, stages, users_recents. Personal API-token auth via `?api_token=`; `additional_data.pagination` paginator.
- `notion` — users, databases, pages, blocks, comments. POST-body cursor for `/v1/search`; 404 soft-fail on unshared resources; opaque `properties`/`created_by`/`last_edited_by` stored as `data_type: json`.
- `github` — organizations, users, repositories, pull_requests, commits, pull_request_commits, pull_request_stats. Dual auth: GitHub App (RS256 JWT + installation token) OR Personal Access Token.
- `quickbooks` — 24 entities. OAuth 2.0 refresh-token grant with **rotating** refresh tokens (`on_token_rotation` callback fires synchronously after each refresh).

### Fixed (per-source bugs spotted during porting)
- All sources: `update_request` paginator hooks now typed against `requests.Request` (the pre-send object) instead of `PreparedRequest`. Matches dlt's `BasePaginator` contract.
- `github`: organization-resource column hints now `nullable: True` to avoid NOT NULL constraint failures on empty syncs.

## [0.1.0a1] — 2026-05-21

### Added
- `attio` source with `companies`, `people`, `deals`, `lists`, `notes` resources.
- Scope-aware 403 handling: missing OAuth scopes skip the affected resource
  with a `WARNING` log and continue.
- `AttioRecordCursorPaginator` for body-cursor pagination on records endpoints.
- `active_scalar` / `promote_active_values` transforms to hoist Attio's
  versioned `values.<attr>[active]` arrays to top-level columns.
