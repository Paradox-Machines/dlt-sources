# github

GitHub dlt source — extracts org members, repositories, pull requests, commits,
and related detail records from the [GitHub REST API](https://docs.github.com/en/rest).

## Resources

| Resource | Primary key | Write disposition | Notes |
|---|---|---|---|
| `organizations` | `id` | `replace` | GET `/orgs/{org}` — one row per configured org |
| `users` | `id` | `replace` | GET `/orgs/{org}/members` — (user, org) pairs |
| `repositories` | `id` | `append` | GET `/orgs/{org}/repos` — incremental on `updated_at` |
| `pull_requests` | `id` | `append` | GET `/repos/{full_name}/pulls` — incremental on `updated_at` |
| `commits` | `sha` | `append` | GET `/repos/{full_name}/commits` — incremental on `commit.committer.date` |
| `pull_request_commits` | `[repository, pull_number, sha]` | `append` | Transformer fed from `pull_requests` |
| `pull_request_stats` | `id` | `append` | Transformer — fetches PR detail (diff stats) for each PR |

## Auth — choose exactly one method

Operators pick one auth method at provision time.  The source detects which
method is active based on which credentials are provided.

---

### Method 1: GitHub App (recommended)

A GitHub App installation provides fine-grained, org-scoped permissions that
can be reviewed and rotated without tying credentials to a human user account.

**How it works:**  The source mints a short-lived JWT (signed with the App's
RSA private key, valid 9 minutes), exchanges it at
`/app/installations/<id>/access_tokens` for an installation access token
(valid ~1 hour, cached ~50 minutes), and sends that token as
`Authorization: Bearer <token>` on every data request.

**Setup:**

1. Create a GitHub App in your organisation (Settings → Developer settings →
   GitHub Apps → New GitHub App).
2. Grant the minimum required permissions:
   - Organisation permissions: **Members → Read** (for `users`), **Metadata → Read**.
   - Repository permissions: **Contents → Read** (for `commits`, `pull_request_commits`),
     **Pull requests → Read** (for `pull_requests`, `pull_request_stats`),
     **Metadata → Read** (for `repositories`, `organizations`).
3. Generate a private key (PEM format) and download it.
4. Install the App on the target org(s) and note the **Installation ID** (visible
   in the URL: `https://github.com/organizations/<org>/settings/installations/<id>`).
5. Note the **App ID** shown on the App's settings page.

**`.dlt/secrets.toml`:**

```toml
[sources.github]
app_id        = "123456"
installation_id = "78901234"
private_key   = """
-----BEGIN RSA PRIVATE KEY-----
MIIEo...
-----END RSA PRIVATE KEY-----
"""
```

---

### Method 2: Personal Access Token (PAT)

Simpler to set up; credentials are tied to one GitHub user account and its
org memberships/permissions.

**Setup:**

1. In GitHub, go to **Settings → Developer settings → Personal access tokens**.
2. Create a classic token (or fine-grained token) with the scopes required by
   the resources you want to extract:
   - `read:org` — `organizations`, `users`
   - `repo` (or `public_repo`) — `repositories`, `pull_requests`, `commits`,
     `pull_request_commits`, `pull_request_stats`
3. Copy the token.

**`.dlt/secrets.toml`:**

```toml
[sources.github]
pat_token = "ghp_..."
```

---

## Config

```toml
# .dlt/config.toml
[sources.github]
org_logins = ["my-org", "another-org"]
```

## Example

```python
import dlt
from paradox_dlt_sources.github import github_source

pipeline = dlt.pipeline(
    pipeline_name="github_demo",
    destination="duckdb",
    dataset_name="github_data",
)

# credentials + org_logins resolved from .dlt/secrets.toml + config.toml
info = pipeline.run(github_source())
print(info)
```

Explicit credentials (useful in tests or notebook environments):

```python
# App auth
info = pipeline.run(
    github_source(
        org_logins=["my-org"],
        app_id="123456",
        installation_id="78901234",
        private_key=open("private-key.pem").read(),
    )
)

# PAT auth
info = pipeline.run(
    github_source(
        org_logins=["my-org"],
        pat_token="ghp_...",
    )
)
```

## Permission errors (403/404)

The source soft-fails on `403` and `404` at the per-org and per-repo level
rather than aborting the entire extract.  This covers:

- App installations that grant org-level permissions but lack repo-level
  `Contents: read` (commits will be skipped for those repos).
- PATs whose owner lost access to an org or repo mid-sync.
- Revoked App installations.

Skipped resources are logged at `WARNING` level with the affected org/repo
path.  Other resources (and other orgs/repos) continue unaffected.

## Empty and template repositories

Repositories with `pushed_at = null` or `size = 0` are excluded from the
`commits` and `pull_requests` crawl.  GitHub returns `409 Git Repository is
empty` on those endpoints for empty repos, which would otherwise abort the
extract.  The `repositories` resource still captures their metadata.

## Known limitations

- `pull_request_stats` fetches one additional HTTP request per pull request
  (the PR detail endpoint includes diff stats not present in the list
  response).  For organisations with large PR volumes, this multiplies
  request count; consider disabling this resource by filtering the returned
  list if rate limits are a concern.
- Incremental resources use `write_disposition="append"` with `start_value`
  (not `last_value`) to avoid silent data loss when multiple pages are fetched
  within a single pipeline run.
