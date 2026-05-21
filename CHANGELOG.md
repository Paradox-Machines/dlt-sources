# Changelog

All notable changes to `paradoxmachines-dlt-sources` are documented here.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
and the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

## [Unreleased]

## [0.1.0a1] — 2026-05-21

### Added
- `attio` source with `companies`, `people`, `deals`, `lists`, `notes` resources.
- Scope-aware 403 handling: missing OAuth scopes skip the affected resource
  with a `WARNING` log and continue.
- `AttioRecordCursorPaginator` for body-cursor pagination on records endpoints.
- `active_scalar` / `promote_active_values` transforms to hoist Attio's
  versioned `values.<attr>[active]` arrays to top-level columns.
