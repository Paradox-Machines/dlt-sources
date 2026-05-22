"""GitHub dlt source — 7 resources matching the Airbyte ``github`` connector.

Resources: ``organizations``, ``users``, ``repositories``, ``pull_requests``,
``commits``, ``pull_request_commits`` (transformer), ``pull_request_stats``
(transformer).

**Authentication** — two mutually-exclusive methods; operators pick one at
provision time:

1. **GitHub App** (preferred): supply ``app_id``, ``installation_id``, and
   ``private_key``.  The source mints a short-lived JWT, exchanges it for an
   installation access token (~1 h, cached ~50 min), and uses that as a Bearer
   token on every request.

2. **Personal Access Token**: supply ``pat_token`` only.  The static token is
   attached directly as a Bearer header.  Simpler to set up; tied to one user.

Which method is active is determined by which parameters are provided — see
``github_source`` for the dispatch logic.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
from requests.exceptions import HTTPError

from .helpers import _HTTP_FORBIDDEN, GitHubAppAuth, GitHubPATAuth, make_client
from .settings import EPOCH_ISO

Row = dict[str, Any]

# Column hints pin scalar fields that may legitimately be NULL for some
# GitHub responses (e.g. an org with no `name` set, only `login`). dlt drops
# all-NULL columns from a batch's parquet schema, which then makes downstream
# staging models fail with "Referenced column not found". Hints force the
# column to materialise even when every row's value is NULL. Only the fields
# the staging model selects are declared here; extra response fields continue
# to flow through schema inference.
_ORGANIZATIONS_COLUMNS: dict[str, dict[str, Any]] = {
    "id": {"data_type": "bigint", "nullable": True},
    "login": {"data_type": "text", "nullable": True},
    "node_id": {"data_type": "text", "nullable": True},
    "name": {"data_type": "text", "nullable": True},
    "description": {"data_type": "text", "nullable": True},
    "created_at": {"data_type": "timestamp", "nullable": True},
}


@dlt.source(name="github")
def github_source(  # noqa: PLR0915
    org_logins: list[str] = dlt.config.value,
    app_id: str | None = None,
    installation_id: str | None = None,
    private_key: str | None = None,
    pat_token: str | None = None,
) -> list[Any]:
    """GitHub source factory.

    Auth dispatch:

    - All four auth params ``None`` → decorator-time placeholder (Dagster
      introspects the resource graph at import time without credentials; any
      real pipeline run supplies credentials via ``dlt.secrets``).
    - ``pat_token`` set, App params ``None`` → PAT auth.
    - All three App params (``app_id``, ``installation_id``, ``private_key``)
      set, ``pat_token`` ``None`` → GitHub App auth.
    - Mixed / partial → ``ValueError``.

    Args:
        org_logins: GitHub organisation logins to crawl (e.g.
            ``["my-org", "another-org"]``).  Resolved from
            ``config["sources.github.org_logins"]`` by default.
        app_id: GitHub App ID.  Resolved from secrets when using App auth.
        installation_id: GitHub App installation ID.  Resolved from secrets.
        private_key: PEM-encoded RSA private key for the App.  Resolved from
            secrets.
        pat_token: GitHub Personal Access Token.  Resolved from secrets when
            using PAT auth.
    """
    # Two mutually-exclusive auth methods. `runtime_kwargs(bu)` in the
    # internal pipeline resolves the active one's secrets from SM and passes
    # only those; callers from tests pass credentials explicitly. Validating
    # here (rather than at module import time) lets the source be instantiated
    # with either set.
    #
    # Special case: Dagster's `_decorator_time_kwargs` calls this factory at
    # code-server boot with no auth kwargs at all so it can introspect the
    # resource graph for the asset map. The resulting source is never used to
    # make HTTP calls — production runs always re-invoke the factory with real
    # credentials. Treat "all four None" as the decorator-time path and build
    # a dummy client; any other partial set is a real misconfiguration.
    auth: GitHubAppAuth | GitHubPATAuth
    if all(x is None for x in (app_id, installation_id, private_key, pat_token)):
        auth = GitHubPATAuth("decorator-time-placeholder")
    elif pat_token is not None:
        if any(x is not None for x in (app_id, installation_id, private_key)):
            raise ValueError(
                "GitHub source received both PAT and App credentials. "
                "Provide exactly one auth method: set `pat_token`, OR all "
                "of `app_id` / `installation_id` / `private_key`."
            )
        auth = GitHubPATAuth(pat_token)
    elif app_id is not None and installation_id is not None and private_key is not None:
        auth = GitHubAppAuth(app_id, installation_id, private_key)
    else:
        raise ValueError(
            "GitHub source needs either a Personal Access Token (`pat_token`) "
            "or full GitHub App credentials (all of `app_id`, "
            "`installation_id`, `private_key`). Got: "
            f"app_id={'set' if app_id is not None else 'missing'}, "
            f"installation_id={'set' if installation_id is not None else 'missing'}, "
            f"private_key={'set' if private_key is not None else 'missing'}, "
            f"pat_token={'set' if pat_token is not None else 'missing'}."
        )
    client = make_client(auth)

    # Both `pull_requests` and `commits` (plus the transformers fed from them)
    # iterate every repo across every configured org. The org→repos listing is
    # cached on the source closure so the resources share one crawl per sync
    # rather than re-paginating `/orgs/{org}/repos` three times.
    _repo_names_cache: list[str] | None = None

    def _repo_full_names() -> list[str]:
        nonlocal _repo_names_cache
        if _repo_names_cache is None:
            # Skip never-pushed repos (`pushed_at=null`, `size=0`). GitHub
            # returns 409 "Git Repository is empty" on `/commits` and `/pulls`
            # for these, which would abort the whole extract. Template repos
            # and Lovable.dev scaffolds are common offenders under broad crawls.
            #
            # 403/404 on `/orgs/{org}/repos` covers orgs where the App has
            # "Metadata: read" permission but lacks "Repositories: read", or
            # where the installation was revoked. Skip that org rather than
            # abort the whole extract.
            names: list[str] = []
            for org in org_logins:
                try:
                    for page in client.paginate(
                        f"/orgs/{org}/repos",
                        params={"type": "all", "per_page": 100},
                    ):
                        for repo in page:
                            if repo.get("pushed_at") and (repo.get("size") or 0) > 0:
                                names.append(repo["full_name"])
                except HTTPError as exc:
                    if exc.response is not None and exc.response.status_code in (403, 404):
                        continue
                    raise
            _repo_names_cache = names
        return _repo_names_cache

    @dlt.resource(
        name="organizations",
        primary_key="id",
        write_disposition="replace",
        columns=_ORGANIZATIONS_COLUMNS,
    )
    def organizations() -> Iterator[Row]:
        # 403/404 covers orgs where the App installation was revoked or never
        # granted; skip rather than abort the whole extract.
        for org in org_logins:
            try:
                yield client.get(f"/orgs/{org}").json()
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (403, 404):
                    continue
                raise

    @dlt.resource(name="users", primary_key="id", write_disposition="replace")
    def users() -> Iterator[Row]:
        # Same user can belong to multiple orgs; emit each (user, org) pair so
        # staging can pick a canonical org via dedup.
        # `/orgs/{org}/members` requires the GitHub App's `Members: read` org
        # permission — not granted by default. Skip silently on 403 so missing
        # scope doesn't kill the whole pipeline.
        for org in org_logins:
            try:
                for page in client.paginate(f"/orgs/{org}/members", params={"per_page": 100}):
                    for member in page:
                        yield {**member, "organization": org}
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code == _HTTP_FORBIDDEN:
                    continue
                raise

    @dlt.resource(
        name="repositories",
        primary_key="id",
        write_disposition="append",
    )
    def repositories(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "updated_at", initial_value=EPOCH_ISO, range_start="open"
        ),
    ) -> Iterator[Row]:
        # `start_value` (prior run's snapshot) — `last_value` would shift
        # mid-loop and silently drop later in-run items.
        threshold = cursor.start_value
        # 403/404 on `/orgs/{org}/repos`: App lacks "Repositories: read"
        # at the org level, or the installation was revoked. Skip that org.
        for org in org_logins:
            stop = False
            try:
                for page in client.paginate(
                    f"/orgs/{org}/repos",
                    params={
                        "type": "all",
                        "sort": "updated",
                        "direction": "desc",
                        "per_page": 100,
                    },
                ):
                    for repo in page:
                        if repo["updated_at"] <= threshold:
                            stop = True
                            break
                        yield repo
                    if stop:
                        break
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (403, 404):
                    continue
                raise

    @dlt.resource(
        name="pull_requests",
        primary_key="id",
        write_disposition="append",
    )
    def pull_requests(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "updated_at", initial_value=EPOCH_ISO, range_start="open"
        ),
    ) -> Iterator[Row]:
        threshold = cursor.start_value
        for full_name in _repo_full_names():
            stop = False
            # 409 covers empty repos that slipped past `_repo_full_names()`
            # (race between listing and per-repo call); 404 covers repos
            # deleted mid-sync. Either one should not kill the whole extract.
            try:
                for page in client.paginate(
                    f"/repos/{full_name}/pulls",
                    params={
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                        "per_page": 100,
                    },
                ):
                    for pr in page:
                        if pr["updated_at"] <= threshold:
                            stop = True
                            break
                        yield pr
                    if stop:
                        break
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (404, 409):
                    continue
                raise

    @dlt.resource(
        name="commits",
        primary_key="sha",
        write_disposition="append",
    )
    def commits(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "commit.committer.date",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        # See `repositories` for the start_value rationale.
        since = cursor.start_value
        for full_name in _repo_full_names():
            # See `pull_requests` — same 409/404 carve-out for races past
            # `_repo_full_names()`'s pre-filter.
            try:
                for page in client.paginate(
                    f"/repos/{full_name}/commits",
                    params={"since": since, "per_page": 100},
                ):
                    for commit in page:
                        yield {**commit, "repository": full_name}
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (404, 409):
                    continue
                raise

    @dlt.transformer(
        data_from=pull_requests,
        name="pull_request_commits",
        primary_key=["repository", "pull_number", "sha"],
        write_disposition="append",
    )
    def pull_request_commits(pr: Row) -> Iterator[Row]:
        full_name = pr["base"]["repo"]["full_name"]
        number = pr["number"]
        # An App installation that grants `Pull requests: read` on an org but
        # not `Contents: read` for a specific repo will list the PR
        # successfully and then 403 on `/pulls/{n}/commits`. 404/409 cover
        # repos deleted or emptied mid-sync. Skip the offending PR rather than
        # abort the whole extract.
        try:
            for page in client.paginate(
                f"/repos/{full_name}/pulls/{number}/commits",
                params={"per_page": 100},
            ):
                for c in page:
                    yield {
                        **c,
                        "repository": full_name,
                        "pull_number": number,
                    }
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (403, 404, 409):
                return
            raise

    @dlt.transformer(
        data_from=pull_requests,
        name="pull_request_stats",
        primary_key="id",
        write_disposition="append",
    )
    def pull_request_stats(pr: Row) -> Iterator[Row]:
        full_name = pr["base"]["repo"]["full_name"]
        number = pr["number"]
        # Same scope mismatch as `pull_request_commits` — App can list the PR
        # but lose access on the detail call. Skip silently.
        try:
            detail = client.get(f"/repos/{full_name}/pulls/{number}").json()
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (403, 404, 409):
                return
            raise
        yield {**detail, "repository": full_name}

    return [
        organizations,
        users,
        repositories,
        pull_requests,
        commits,
        pull_request_commits,
        pull_request_stats,
    ]


__all__ = ["github_source"]
