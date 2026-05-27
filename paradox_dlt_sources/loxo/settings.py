"""Loxo source — endpoint constants and resource configuration."""

from __future__ import annotations

# `app.loxo.co` is the domain for every SaaS agency on Loxo's hosted plan.
# Custom-domain agencies are rare; for those, set `LOXO_API_BASE_URL` to the
# full `https://{custom-domain}/api/{agency_slug}` URL — `_base_url` honors
# that env var ahead of the constructed default.
_DEFAULT_DOMAIN = "app.loxo.co"

# Resources emitted by this source.
RESOURCES: tuple[str, ...] = (
    "people",
    "jobs",
    "companies",
    "deals",
    "activities",
    "users",
)

# Loxo REST endpoints for each resource. `activities` maps to `/person_events`
# (Loxo's URL naming; we keep the friendlier dlt resource name).
ENDPOINT_PEOPLE = "/people"
ENDPOINT_JOBS = "/jobs"
ENDPOINT_COMPANIES = "/companies"
ENDPOINT_DEALS = "/deals"
ENDPOINT_PERSON_EVENTS = "/person_events"
ENDPOINT_USERS = "/users"

# Page size for the `/jobs` endpoint (the only endpoint using PageNumberPaginator).
# Scroll-paginated endpoints (`/people`, `/companies`, `/deals`, `/person_events`)
# reject `per_page` with 422 on `/companies` and `/deals`, so we omit it there
# and rely on Loxo's server-side default page size.
JOBS_PAGE_SIZE = 100
