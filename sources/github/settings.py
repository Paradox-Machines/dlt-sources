"""GitHub source — endpoint constants, resource list, JWT defaults."""

from __future__ import annotations

import os

GITHUB_API_BASE_URL = os.environ.get("GITHUB_API_BASE_URL", "https://api.github.com")

# GitHub caps App JWTs at 10 minutes; stay 1 minute under.
GITHUB_APP_JWT_TTL_SECONDS = 9 * 60

# GitHub issues installation tokens valid for ~1 hour; refresh at 50 minutes.
GITHUB_INSTALLATION_TOKEN_TTL_SECONDS = 50 * 60

# Epoch ISO used as the initial cursor value for incremental resources.
EPOCH_ISO = "1970-01-01T00:00:00Z"

# Canonical list of resource names this source produces.
RESOURCE_NAMES = (
    "organizations",
    "users",
    "repositories",
    "pull_requests",
    "commits",
    "pull_request_commits",
    "pull_request_stats",
)
