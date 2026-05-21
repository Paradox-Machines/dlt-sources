# Changelog

All notable changes to `paradoxmachines-dlt-sources` are documented here.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
and the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

## [Unreleased]

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
