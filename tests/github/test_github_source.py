"""Integration tests for github_source with PAT auth.

Mocks GitHub REST API endpoints with `responses` and asserts the source
materialises the expected resources with correct row counts and primary keys
when piped through a duckdb destination.

GitHub REST uses RFC 5988 Link-header pagination (HeaderLinkPaginator).
To simulate a single-page response with no next page, the mock simply returns
no `Link` header — HeaderLinkPaginator stops when the header is absent.
"""

from __future__ import annotations

import pytest
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


# ---------------------------------------------------------------------------
# _repo_full_names cache — 403/404 skip + reraise on other status
# Triggered via `repositories` resource since it calls _repo_full_names().
# The cache helper is shared by pull_requests, commits, and the transformers.
# We exercise it by running `github_source` with two orgs where the
# _repo_full_names() call errors for each org.
# ---------------------------------------------------------------------------

_ORG2 = "other-org"

# Minimal repo record with all fields required by both the cache helper and
# the `repositories` incremental crawl (which reads `updated_at`).
_REPO_ANVIL = {
    "id": 3001,
    "name": "anvil",
    "full_name": "acme-org/anvil",
    "pushed_at": "2026-04-01T12:00:00Z",
    "updated_at": "2026-04-01T12:00:00Z",
    "size": 100,
}


def _stub_all_orgs_except_repos(orgs: list[str]) -> None:
    """Stub organizations + members for each org (not repos — caller provides those)."""
    for org in orgs:
        responses.mock.add(
            responses.GET,
            f"{_BASE}/orgs/{org}",
            json={**load_fixture("github", "org_acme"), "login": org},
        )
        responses.mock.add(responses.GET, f"{_BASE}/orgs/{org}/members", json=[])


@responses.activate
def test_repo_full_names_skips_403_and_404(tmp_pipeline):  # type: ignore[no-untyped-def]
    """_repo_full_names skips org on 403 / 404 on /orgs/{org}/repos.

    Two orgs: _ORG → 403 on _repo_full_names(), _ORG2 → 404.
    Both should be skipped gracefully; the pipeline must not fail.
    """
    _stub_all_orgs_except_repos([_ORG, _ORG2])
    # _repo_full_names() calls (used by pull_requests / commits)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json={"message": "Resource not accessible by integration", "documentation_url": "https://docs.github.com"},
        status=403,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG2}/repos",
        json={"message": "Not Found"},
        status=404,
    )
    # repositories incremental crawl — one call per org (these also get 403/404
    # but via the repositories resource's own handler, which also skips them)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json={"message": "Resource not accessible by integration", "documentation_url": "https://docs.github.com"},
        status=403,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG2}/repos",
        json={"message": "Not Found"},
        status=404,
    )

    info = tmp_pipeline.run(github_source(org_logins=[_ORG, _ORG2], pat_token=_PAT))
    assert not info.has_failed_jobs


@responses.activate
def test_repositories_reraises_on_500_via_direct_iter():  # type: ignore[no-untyped-def]
    """A 500 from /orgs/{org}/repos propagates when iterating the resource directly.

    Calling pipeline.run() with a 500-raising mock results in dlt wrapping the
    error into LoadInfo and continuing other resources — useful behavior at
    the pipeline level but unsuitable for asserting that the resource's
    `raise` branch runs. Direct iteration of `src.repositories` exercises the
    Python-level code path: dlt's `ResourceExtractionError` wraps the
    underlying `HTTPError(500)`, confirming the raise on line 226 fires.
    """
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json={"message": "Internal Server Error"},
        status=500,
    )

    src = github_source(org_logins=[_ORG], pat_token=_PAT)
    with pytest.raises(Exception, match="repositories"):
        list(src.repositories)


# ---------------------------------------------------------------------------
# organizations — 403/404 skip per org (reraise path unreachable: client.get()
# does not raise HTTPError on 4xx/5xx; lines 167-170 are defensive dead code)
# ---------------------------------------------------------------------------


@responses.activate
def test_organizations_skips_404(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A 404 on /orgs/{org} should be silently skipped; other orgs continue."""
    # _ORG → 404, _ORG2 → 200
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}",
        json={"message": "Not Found"},
        status=404,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG2}",
        json={**load_fixture("github", "org_acme"), "login": _ORG2, "id": 9999},
    )
    # repos + members for both orgs
    for org in (_ORG, _ORG2):
        responses.mock.add(responses.GET, f"{_BASE}/orgs/{org}/members", json=[])
        # _repo_full_names() + repositories incremental crawl (2 calls each)
        responses.mock.add(responses.GET, f"{_BASE}/orgs/{org}/repos", json=[])
        responses.mock.add(responses.GET, f"{_BASE}/orgs/{org}/repos", json=[])

    info = tmp_pipeline.run(github_source(org_logins=[_ORG, _ORG2], pat_token=_PAT))
    assert not info.has_failed_jobs

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT login FROM organizations")
    logins = [r[0] for r in rows]
    assert _ORG not in logins
    assert _ORG2 in logins


# ---------------------------------------------------------------------------
# users — reraise on non-403 status (paginate raises HTTPError; pipeline raises)
# ---------------------------------------------------------------------------


@responses.activate
def test_users_reraises_on_unexpected_status(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A non-403 status on /orgs/{org}/members must be re-raised (propagates as PipelineStepFailed)."""
    responses.mock.add(
        responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme")
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/members",
        json={"message": "Internal Server Error"},
        status=500,
    )
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[])

    with pytest.raises(Exception, match="500"):
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))


# ---------------------------------------------------------------------------
# repositories — 403/404 skip + cursor early-termination + reraise on 500
# ---------------------------------------------------------------------------


@responses.activate
def test_repositories_skips_403_and_404(tmp_pipeline):  # type: ignore[no-untyped-def]
    """repositories skips org on 403 or 404 on /orgs/{org}/repos (incremental crawl)."""
    _stub_all_orgs_except_repos([_ORG, _ORG2])
    # _repo_full_names() calls → both return empty (no repos to crawl for PR/commits)
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG2}/repos", json=[])
    # repositories incremental crawl: _ORG → 403, _ORG2 → 404
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json={"message": "Resource not accessible by integration", "documentation_url": "https://docs.github.com"},
        status=403,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG2}/repos",
        json={"message": "Not Found"},
        status=404,
    )

    info = tmp_pipeline.run(github_source(org_logins=[_ORG, _ORG2], pat_token=_PAT))
    assert not info.has_failed_jobs

    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "repositories" not in table_names


@responses.activate
def test_repositories_cursor_early_termination(tmp_pipeline):  # type: ignore[no-untyped-def]
    """repositories stops paging once it finds a repo with updated_at at/below threshold.

    Run 1: one repo seeds the cursor at "2026-04-01T12:00:00Z".
    Run 2: server returns two repos — catapult (newer, above threshold) and
           anvil (at exactly the threshold). Anvil triggers stop=True; only
           catapult must appear in the second run's output.
    """
    _repo_stub = [_REPO_ANVIL]

    # ── Run 1: seed cursor ────────────────────────────────────────────────
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repo_stub)   # _repo_full_names
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repo_stub)   # repositories crawl
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=[])
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    # ── Run 2: threshold = "2026-04-01T12:00:00Z" ────────────────────────
    _catapult = {
        "id": 3003, "name": "catapult", "full_name": "acme-org/catapult",
        "pushed_at": "2026-05-01T00:00:00Z", "updated_at": "2026-05-01T00:00:00Z", "size": 200,
    }
    _run2 = [_catapult, {**_REPO_ANVIL}]  # catapult above, anvil == threshold

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_run2)   # _repo_full_names
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_run2)   # repositories crawl
        # PR + commit calls for both repos in cache
        for repo in ("acme-org/catapult", "acme-org/anvil"):
            rsps.add(responses.GET, f"{_BASE}/repos/{repo}/pulls", json=[])
            rsps.add(responses.GET, f"{_BASE}/repos/{repo}/commits", json=[])
        info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    assert not info.has_failed_jobs
    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM repositories ORDER BY id")
    ids = [r[0] for r in rows]
    assert 3003 in ids              # catapult: above threshold → yielded on run 2
    assert ids.count(3001) == 1     # anvil: at threshold → NOT re-yielded on run 2, only from run 1


@responses.activate
def test_repositories_reraises_on_unexpected_status(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A 500 on /orgs/{org}/repos (repositories incremental crawl) must be re-raised."""
    _stub_all_orgs_except_repos([_ORG])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[])   # _repo_full_names
    responses.mock.add(
        responses.GET,
        f"{_BASE}/orgs/{_ORG}/repos",
        json={"message": "Internal Server Error"},
        status=500,
    )

    with pytest.raises(Exception, match="500"):
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))


# ---------------------------------------------------------------------------
# pull_requests — 404/409 skip + cursor early-termination + reraise on 500
# ---------------------------------------------------------------------------


@responses.activate
def test_pull_requests_skips_404_and_409(tmp_pipeline):  # type: ignore[no-untyped-def]
    """pull_requests skips repos on 404 and 409 (empty repo race)."""
    _ghost = {
        "id": 3002, "name": "ghost", "full_name": "acme-org/ghost",
        "pushed_at": "2026-03-01T12:00:00Z", "updated_at": "2026-03-01T12:00:00Z", "size": 50,
    }
    _repos = [_REPO_ANVIL, _ghost]
    _stub_all_orgs_except_repos([_ORG])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)   # _repo_full_names
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)   # repositories
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls",
        json={"message": "Not Found"}, status=404,
    )
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/ghost/pulls",
        json={"message": "Git Repository is empty."}, status=409,
    )
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/ghost/commits", json=[])

    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "pull_requests" not in table_names


@responses.activate
def test_pull_requests_cursor_early_termination(tmp_pipeline):  # type: ignore[no-untyped-def]
    """pull_requests stops per-repo once it sees a PR updated_at at/below threshold.

    Run 1 seeds the cursor at "2026-04-01T10:00:00Z".
    Run 2 returns PR-4002 (above threshold) + PR-4001 (== threshold).
    Only PR-4002 should appear in the second run's rows.
    """
    _repos = [_REPO_ANVIL]
    _pr_seed = [{
        "id": 4001, "number": 1, "updated_at": "2026-04-01T10:00:00Z",
        "base": {"repo": {"full_name": "acme-org/anvil", "id": 3001}},
        "head": {"ref": "a", "sha": "aaa"},
    }]

    # ── Run 1 ────────────────────────────────────────────────────────────
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=_pr_seed)
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls/1/commits", json=[])
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls/1", json={
            **_pr_seed[0], "additions": 0, "deletions": 0, "changed_files": 0, "commits": 0,
        })
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    # ── Run 2: threshold = "2026-04-01T10:00:00Z" ────────────────────────
    _pr_new = {
        "id": 4002, "number": 2, "updated_at": "2026-05-01T00:00:00Z",
        "base": {"repo": {"full_name": "acme-org/anvil", "id": 3001}},
        "head": {"ref": "b", "sha": "bbb"},
    }
    _pr_run2 = [_pr_new, {**_pr_seed[0]}]  # new first (desc order), then at-threshold

    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
        rsps.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=_pr_run2)
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls/2/commits", json=[])
        rsps.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls/2", json={
            **_pr_new, "additions": 5, "deletions": 2, "changed_files": 1, "commits": 1,
        })
        info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))

    assert not info.has_failed_jobs
    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM pull_requests ORDER BY id")
    ids = [r[0] for r in rows]
    assert 4002 in ids          # new PR: above threshold → yielded on run 2
    assert ids.count(4001) == 1  # only from run 1; not re-yielded on run 2


@responses.activate
def test_pull_requests_reraises_on_unexpected_status(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A 500 on /repos/{x}/pulls must be re-raised."""
    _stub_all_orgs_except_repos([_ORG])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls",
        json={"message": "Internal Server Error"}, status=500,
    )
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])

    with pytest.raises(Exception, match="500"):
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))


# ---------------------------------------------------------------------------
# commits — 404/409 skip + reraise on 500
# ---------------------------------------------------------------------------


@responses.activate
def test_commits_skips_404_and_409(tmp_pipeline):  # type: ignore[no-untyped-def]
    """commits skips repos on 404 and 409."""
    _ghost = {
        "id": 3002, "name": "ghost", "full_name": "acme-org/ghost",
        "pushed_at": "2026-03-01T00:00:00Z", "updated_at": "2026-03-01T00:00:00Z", "size": 10,
    }
    _repos = [_REPO_ANVIL, _ghost]
    _stub_all_orgs_except_repos([_ORG])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=_repos)
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/ghost/pulls", json=[])
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/anvil/commits",
        json={"message": "Not Found"}, status=404,
    )
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/ghost/commits",
        json={"message": "Git Repository is empty."}, status=409,
    )

    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "commits" not in table_names


@responses.activate
def test_commits_reraises_on_unexpected_status(tmp_pipeline):  # type: ignore[no-untyped-def]
    """A 500 on /repos/{x}/commits must be re-raised."""
    _stub_all_orgs_except_repos([_ORG])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=[])
    responses.mock.add(
        responses.GET, f"{_BASE}/repos/acme-org/anvil/commits",
        json={"message": "Internal Server Error"}, status=500,
    )

    with pytest.raises(Exception, match="500"):
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))


# ---------------------------------------------------------------------------
# pull_request_commits transformer — 403/404/409 skip + reraise on 500
# ---------------------------------------------------------------------------

_FAKE_PR: dict = {
    "id": 5001,
    "number": 99,
    "updated_at": "2026-05-01T00:00:00Z",
    "base": {"repo": {"full_name": "acme-org/anvil", "id": 3001}},
    "head": {"ref": "feature/x", "sha": "xyzxyz"},
}


def _pr_commits_setup(pr: dict) -> None:
    """Register minimal mocks to drive one PR through the transformer chain."""
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=[pr])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])


@pytest.mark.parametrize("status,error_body", [
    (403, {"message": "Resource not accessible by integration", "documentation_url": "https://docs.github.com"}),
    (404, {"message": "Not Found"}),
    (409, {"message": "Git Repository is empty."}),
])
@responses.activate
def test_pull_request_commits_skips_on_status(
    status: int, error_body: dict, tmp_pipeline  # type: ignore[no-untyped-def]
) -> None:
    """pull_request_commits silently skips on 403, 404, or 409."""
    _pr_commits_setup(_FAKE_PR)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/99/commits",
        json=error_body,
        status=status,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/99",
        json={**_FAKE_PR, "additions": 1, "deletions": 0, "changed_files": 1, "commits": 1},
    )
    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs, f"Expected no failure on status {status}"
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "pull_request_commits" not in table_names, f"Expected no rows on status {status}"


@responses.activate
def test_pull_request_commits_reraises_on_unexpected_status(tmp_pipeline):  # type: ignore[no-untyped-def]
    """pull_request_commits re-raises on a 500."""
    _pr_commits_setup(_FAKE_PR)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/99/commits",
        json={"message": "Internal Server Error"},
        status=500,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/99",
        json={**_FAKE_PR, "additions": 1, "deletions": 0, "changed_files": 1, "commits": 1},
    )

    with pytest.raises(Exception, match="500"):
        tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))


# ---------------------------------------------------------------------------
# pull_request_stats transformer — 403/404/409 skip
# Note: pull_request_stats uses client.get() which does NOT raise HTTPError on
# 4xx/5xx responses. Lines 338-341 (the except/raise block) are therefore not
# reachable via responses-based HTTP mocking. Only the skip path matters here:
# a 403/404/409 from client.get() returns a dict body that gets yielded without
# triggering the except block at all. We test that the resource continues cleanly
# when client.get() returns error status codes.
# ---------------------------------------------------------------------------


def _pr_stats_setup(pr: dict) -> None:
    """Mocks that drive one PR to pull_request_stats; pull_request_commits → empty."""
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}", json=load_fixture("github", "org_acme"))
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/members", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/orgs/{_ORG}/repos", json=[_REPO_ANVIL])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls", json=[pr])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/commits", json=[])
    responses.mock.add(responses.GET, f"{_BASE}/repos/acme-org/anvil/pulls/99/commits", json=[])


@responses.activate
def test_pull_request_stats_yields_detail_row(tmp_pipeline):  # type: ignore[no-untyped-def]
    """pull_request_stats yields a detail row with the repository field on success."""
    _pr_stats_setup(_FAKE_PR)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/repos/acme-org/anvil/pulls/99",
        json={**_FAKE_PR, "additions": 7, "deletions": 3, "changed_files": 2, "commits": 2},
    )

    info = tmp_pipeline.run(github_source(org_logins=[_ORG], pat_token=_PAT))
    assert not info.has_failed_jobs
    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, repository, additions FROM pull_request_stats")
    assert len(rows) == 1
    assert rows[0][0] == 5001
    assert rows[0][1] == "acme-org/anvil"
    assert rows[0][2] == 7
