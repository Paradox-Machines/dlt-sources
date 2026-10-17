"""Ellucian Ethos source — endpoints, media types and the Banner resource list.

Everything goes through Ellucian Ethos Integration (Integration Management):

* ``POST /auth`` with ``Authorization: Bearer <api key>`` returns a JWT as the
  response body.  The JWT lives five minutes.
* ``GET /api/{resource}`` is proxied to the authoritative application (Banner).
  Paging is ``offset`` / ``limit``; responses carry ``X-Total-Count`` and
  ``X-Media-Type``.
* ``GET /consume`` returns the change notifications queued for the API key's
  application.  Passing ``lastProcessedID`` acknowledges (removes) every
  message up to that id.

References: "Banner Ethos API - Use" and "Integration Management" (Ellucian,
September 2026), and Ellucian's public Ethos Integration API docs.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_BASE_URL = "https://integrate.elluciancloud.com"
AUTH_PATH = "/auth"
API_PATH = "/api/{resource}"
CONSUME_PATH = "/consume"

# Integration Management access tokens expire 5 minutes after issue.
TOKEN_TTL_SECONDS = 300
TOKEN_REFRESH_SAFETY_SECONDS = 60

# Without a pinned version Ethos serves the latest representation.
LATEST_MEDIA_TYPE = "application/json"
MEDIA_TYPE_TEMPLATE = "application/vnd.hedtech.integration.v{version}+json"
CHANGE_NOTIFICATIONS_MEDIA_TYPE = "application/vnd.hedtech.change-notifications.v2+json"

TOTAL_COUNT_HEADER = "x-total-count"
MAX_PAGE_SIZE_HEADER = "x-max-page-size"
MEDIA_TYPE_HEADER = "x-media-type"
CONTENT_RESTRICTED_HEADER = "x-content-restricted"
REMAINING_HEADER = "x-remaining"

DEFAULT_PAGE_SIZE = 500
DEFAULT_FEED_BATCH_SIZE = 1000
DEFAULT_MAX_ATTEMPTS = 6
MAX_RETRY_DELAY_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_RECONCILIATION_INTERVAL_HOURS = 24

USER_AGENT = "paradox-dlt-sources-ellucian-ethos (+https://github.com/Paradox-Machines/dlt-sources)"

CHANGE_NOTIFICATIONS = "change_notifications"
VERSIONS_SUFFIX = "_versions"

STANDARD_COLUMNS: Mapping[str, str] = {
    "_source_created_at": "timestamp",
    "_source_updated_at": "timestamp",
    "_is_deleted": "bool",
    "_deleted_at": "timestamp",
    "_extracted_at": "timestamp",
    "_ethos_media_type": "text",
}

CHANGE_COLUMNS: Mapping[str, str] = {
    "_change_op": "text",
    "_change_position": "text",
    "_committed_at": "timestamp",
}


@dataclass(frozen=True)
class EthosResource:
    """One Ethos resource loaded as one table.

    Attributes:
        name: Ethos resource name, as in the URL (``section-registrations``).
        restricted: Loaded only when named in ``resources`` (FAFSA data).
    """

    name: str
    restricted: bool = False

    @property
    def table(self) -> str:
        """Destination table: the resource name in snake_case."""
        return self.name.replace("-", "_")


def _r(*names: str, restricted: bool = False) -> tuple[EthosResource, ...]:
    return tuple(EthosResource(n, restricted) for n in names)


# Coverage follows RMU's draft lineage and star schema (Data Book, Enrollment
# Analytics, unified person record, IPEDS).  The Banner table behind each
# resource comes from the "GUID tables" section of "Banner Ethos API - Use".
RESOURCES: tuple[EthosResource, ...] = (
    # People and demographics (SPRIDEN, SPBPERS, GORPRAC, SPRADDR, GORVISA)
    *_r(
        "persons",
        "addresses",
        "person-visas",
        "person-external-education",
        "races",
        "ethnicities",
        "gender-identities",
        "citizenship-statuses",
        "countries",
        "regions",
        "address-types",
        "email-types",
        "phone-types",
        "person-name-types",
        "alternative-credential-types",
    ),
    # Academic structure and calendar (STVTERM, SOBPTRM, SMRPRLE, STVMAJR, STVCIPC)
    *_r(
        "academic-periods",
        "academic-levels",
        "academic-programs",
        "academic-disciplines",
        "academic-credentials",
        "cip-codes",
        "educational-institution-units",
        "educational-institutions",
        "subjects",
        "courses",
        "sections",
        "instructional-methods",
        "instructional-delivery-methods",
        "instructors",
    ),
    # Student record, registrations, outcomes (SGBSTDN, SORLCUR, SFBETRM,
    # SFRSTCR, SHRDGMR, SHRTTRM, SGRCHRT, SGRSPRT, SORTEST)
    *_r(
        "students",
        "student-types",
        "residency-types",
        "student-academic-programs",
        "student-academic-periods",
        "student-academic-period-profiles",
        "student-academic-credentials",
        "student-academic-standings",
        "academic-standings",
        "student-grade-point-averages",
        "student-transcript-grades",
        "student-cohorts",
        "student-cohort-assignments",
        "section-registrations",
        "section-registration-statuses",
        "campus-involvements",
        "campus-organizations",
        "student-aptitude-assessments",
        "aptitude-assessments",
    ),
    # Admissions (SARADAP, SARAPPD)
    *_r(
        "admission-applications",
        "admission-decisions",
        "admission-decision-types",
        "admission-application-types",
        "admission-application-status-types",
        "admission-populations",
    ),
    # Financial aid (RPRAWRD, RFRBASE, ROBINST, RORTPRD)
    *_r(
        "student-financial-aid-awards",
        "restricted-student-financial-aid-awards",
        "financial-aid-funds",
        "financial-aid-fund-categories",
        "financial-aid-fund-classifications",
        "financial-aid-years",
        "financial-aid-award-periods",
    ),
    # FAFSA applications (RCRAPP1): income and family data, Restricted tier.
    *_r("financial-aid-applications", "financial-aid-application-outcomes", restricted=True),
    # Student accounts (TBRACCD, TBBDETC)
    *_r("student-charges", "student-payments", "accounting-codes"),
)

RESOURCES_BY_NAME: Mapping[str, EthosResource] = {r.name: r for r in RESOURCES}
