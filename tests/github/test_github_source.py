"""Integration tests for github_source with PAT auth.

Mocks GitHub REST API endpoints with `responses` and asserts the source
materialises the expected resources with correct row counts and primary keys
when piped through a duckdb destination.

GitHub REST uses RFC 5988 Link-header pagination (HeaderLinkPaginator).
To simulate a single-page response with no next page, the mock simply returns
no `Link` header — HeaderLinkPaginator stops when the header is absent.
"""

from __future__ import annotations

import responses

from paradox_dlt_sources.github import github_source
from tests._helpers.fixture_loader import load_fixture

_BASE = "https://api.github.com"
_PAT = "ghp_integration_test_token"
_ORG = "acme-org"


def _register_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned GitHub API responses for all 7 resources."""
    # organizations
    rsps.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}",
        json=load_fixture("github", "org_acme"),
        status=200,
    )
    # users (members)
    rsps.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/members",
        json=load_fixture("github", "org_members"),
        status=200,
    )
    # repositories — two calls: one for _repo_full_names() cache, one for
    # the `repositories` resource incremental crawl (both use same fixture).
    rsps.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json=load_fixture("github", "repos"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json=load_fixture("github", "repos"),
        status=200,
    )
    # pull_requests — two repos; anvil has PRs, roadrunner-trap has none
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls",
        json=load_fixture("github", "pulls_anvil"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/roadrunner-trap/pulls",
        json=[],
        status=200,
    )
    # commits
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/commits",
        json=load_fixture("github", "commits_anvil"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/roadrunner-trap/commits",
        json=[],
        status=200,
    )
    # pull_request_commits (transformer feeds from pull_requests)
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/1/commits",
        json=load_fixture("github", "pr_commits_anvil_1"),
        status=200,
    )
    # pull_request_stats (transformer — fetches PR detail)
    rsps.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/1",
        json=load_fixture("github", "pr_detail_anvil_1"),
        status=200,
    )


@responses.activate
def test_github_source_runs_against_duckdb(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"organizations", "users", "repositories", "pull_requests", "commits"} <= table_names


@responses.activate
def test_organizations_resource_yields_one_row_per_org(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, login, name FROM organizations")
    assert len(rows) == 1
    assert rows[0][0] == 1001
    assert rows[0][1] == "acme-org"
    assert rows[0][2] == "Acme Corp"


@responses.activate
def test_users_resource_yields_members_with_org_field(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT login, organization FROM users ORDER BY login")
    assert len(rows) == 2
    assert rows[0] == ("alice", _ORG)
    assert rows[1] == ("bob", _ORG)


@responses.activate
def test_repositories_resource_yields_repos(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, full_name FROM repositories ORDER BY id")
    assert len(rows) == 2
    assert rows[0] == (3001, "acme-org/anvil")
    assert rows[1] == (3002, "acme-org/roadrunner-trap")


@responses.activate
def test_pull_requests_resource_yields_prs(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, number FROM pull_requests")
    assert len(rows) == 1
    assert rows[0] == (4001, 1)


@responses.activate
def test_commits_resource_yields_commits_with_repo_field(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT sha, repository FROM commits ORDER BY sha")
    # 2 commits from anvil fixture
    assert len(rows) == 2
    for _, repo in rows:
        assert repo == "acme-org/anvil"


@responses.activate
def test_pull_request_commits_transformer_adds_pr_context(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT sha, repository, pull_number FROM pull_request_commits")
    assert len(rows) == 1
    assert rows[0] == ("abc123", "acme-org/anvil", 1)


@responses.activate
def test_pull_request_stats_transformer_yields_detail_with_repo(tmp_pipeline):  # type: ignore[no-untyped-def]
    _register_mocks(responses.mock)
    tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql(
            "SELECT id, repository, additions, deletions FROM pull_request_stats"
        )
    assert len(rows) == 1
    assert rows[0][0] == 4001
    assert rows[0][1] == "acme-org/anvil"
    assert rows[0][2] == 42
    assert rows[0][3] == 5


@responses.activate
def test_organizations_skips_403(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A 403 on /orgs/{org} should be silently skipped."""
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}",
        json={"message": "Forbidden"},
        status=403,
    )
    # _repo_full_names() is called by pull_requests/commits
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[], status=200)
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[], status=200)
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[], status=200)

    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs


@responses.activate
def test_users_skips_403(tmp_pipeline):  # type: ignore[no-untyped-def]
    """Missing Members:read permission (403) on /members should be skipped."""
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}",
        json=load_fixture("github", "org_acme"),
    )
    # Members endpoint returns 403
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/members",
        json={"message": "Forbidden"},
        status=403,
    )
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[], status=200)
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[], status=200)

    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs
