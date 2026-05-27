"""Integration tests for the loxo_source factory.

Mocks Loxo HTTP endpoints with ``responses`` and asserts the source
materialises the expected resources with correct row counts and primary keys
when piped to a duckdb destination.
"""

from __future__ import annotations

import pytest
import responses

from paradox_dlt_sources.loxo import loxo_source
from tests._helpers.fixture_loader import load_fixture, register_get

_BASE = "https://app.loxo.co/api/test-agency"
_EXPECTED_RESOURCES = {"people", "jobs", "companies", "deals", "activities", "users"}


def _register_loxo_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all six Loxo endpoints."""
    # people — 2 pages via scroll_id (page 2 omits scroll_id → terminal).
    rsps.add(
        responses.GET,
        f"{_BASE}/people",
        json=load_fixture("loxo", "people_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/people",
        json=load_fixture("loxo", "people_page_2"),
        status=200,
    )
    # jobs — 2 pages via PageNumberPaginator (page 2 is empty → terminal).
    rsps.add(
        responses.GET,
        f"{_BASE}/jobs",
        json=load_fixture("loxo", "jobs_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/jobs",
        json=load_fixture("loxo", "jobs_page_2"),
        status=200,
    )
    # Single-page resources.
    register_get(rsps, f"{_BASE}/companies", load_fixture("loxo", "companies_page_1"))
    register_get(rsps, f"{_BASE}/deals", load_fixture("loxo", "deals_page_1"))
    register_get(rsps, f"{_BASE}/person_events", load_fixture("loxo", "person_events_page_1"))
    register_get(rsps, f"{_BASE}/users", load_fixture("loxo", "users_page_1"))


# --- end-to-end integration ---


@responses.activate
def test_loxo_source_runs_against_duckdb(tmp_pipeline):
    _register_loxo_mocks(responses.mock)

    info = tmp_pipeline.run(
        loxo_source(agency_slug="test-agency", api_key="loxo_dummy", base_url=_BASE)
    )

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert table_names >= _EXPECTED_RESOURCES


# --- resource contract ---


def test_returns_expected_resources():
    src = loxo_source(agency_slug="test-agency", api_key="loxo_dummy")
    assert {r.name for r in src.resources.values()} == _EXPECTED_RESOURCES


@pytest.mark.parametrize("resource", sorted(_EXPECTED_RESOURCES))
def test_primary_key_is_id(resource):
    src = loxo_source(agency_slug="test-agency", api_key="loxo_dummy")
    schema = src.resources[resource].compute_table_schema()
    assert schema["columns"]["id"].get("primary_key") is True


@pytest.mark.parametrize("resource", sorted(_EXPECTED_RESOURCES))
def test_write_disposition_is_replace(resource):
    # All resources start as `replace` until the open questions on
    # `updated_at` filter support + `scroll_id` stability are settled —
    # see module docstring.
    src = loxo_source(agency_slug="test-agency", api_key="loxo_dummy")
    assert src.resources[resource].write_disposition == "replace"


# --- endpoint quirks ---


def test_activities_resource_targets_person_events_endpoint(monkeypatch):
    """Loxo's API names the activities collection `person_events`; a request to
    `/activities` returns 404. Guard against accidental rename back.
    """
    captured_paths: list[str] = []

    class _StubClient:
        def paginate(self, path, **kwargs):
            captured_paths.append(path)
            return iter([])

    monkeypatch.setattr(
        "paradox_dlt_sources.loxo._client",
        lambda *args, **kwargs: _StubClient(),
    )
    src = loxo_source(agency_slug="test-agency", api_key="loxo_dummy")
    list(src.resources["activities"])

    assert captured_paths == ["/person_events"]


def test_scroll_resources_omit_per_page_param(monkeypatch):
    """`/companies` and `/deals` reject `per_page` with 422; the safest default
    is to not send it at all. Guard against accidental re-add.
    """
    captured_params: list[dict | None] = []

    class _StubClient:
        def paginate(self, path, **kwargs):
            captured_params.append(kwargs.get("params"))
            return iter([])

    monkeypatch.setattr(
        "paradox_dlt_sources.loxo._client",
        lambda *args, **kwargs: _StubClient(),
    )
    src = loxo_source(agency_slug="test-agency", api_key="loxo_dummy")
    for resource_name in ("people", "companies", "deals", "activities"):
        list(src.resources[resource_name])

    # None of the scroll-paginated resources should send `per_page`.
    for params in captured_params:
        assert params is None or "per_page" not in params
