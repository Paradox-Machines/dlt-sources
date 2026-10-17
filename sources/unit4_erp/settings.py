"""Unit4 ERP source — object table and API constants.

Every read goes through the U4 ERPx **Object API**:
``GET {base_url}/{version}/objects/{object}?companyId=&select=&filter=&orderBy=&limit=``.
Responses are a bare JSON array: no envelope, no total count, no next link.

* **Paging is keyset**, never ``offset``: each request orders on the object's
  key fields and filters past the last key read (Unit4's "optimised
  pagination").  Offset paging slows down at high offsets and skips rows when
  data changes between pages.
* **Change capture** is a server-side cursor on ``lastUpdated/updatedAt``
  (Unit4's documented "incremental load").  Objects without ``lastUpdated``
  are read in full every run.
* **Keys** below are the ``x-u4-key`` declared on each object's schema in the
  public ERPx reference (scraped 2026-09-29).  Nested key fields are written
  with ``/`` exactly as the filter syntax expects them.

Reference: https://develop.unit4cloud.com/erpx/reference/object-api/
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

# --- Auth -----------------------------------------------------------------------
# OAuth 2.0 client credentials against Unit4 IDS.  The token URL is tenant
# specific (it comes with the tenant's "instance document"), so it has no default.
DEFAULT_SCOPE = "u4erp"
DEFAULT_TOKEN_TTL_SECONDS = 3600
TOKEN_REFRESH_SAFETY_SECONDS = 60

AuthType = Literal["oauth_client_credentials", "basic"]
AUTH_TYPES: tuple[AuthType, ...] = ("oauth_client_credentials", "basic")

USER_AGENT = "paradox-dlt-sources-unit4-erp (+https://github.com/Paradox-Machines/dlt-sources)"

# --- Limits (https://develop.unit4cloud.com/erpx/limits-and-quotas/) ------------
# 500 requests/minute per tenant across all M2M clients.  Stay below it so other
# integrations on the same tenant keep working.
DEFAULT_MAX_REQUESTS_PER_MINUTE = 450
MAX_RECORDS_PER_CALL = 50_000
DEFAULT_PAGE_SIZE = 10_000
# Unit4 terminates calls after 220 s; give the socket a little more.
REQUEST_TIMEOUT_SECONDS = 240
DEFAULT_MAX_ATTEMPTS = 6
MAX_RETRY_DELAY_SECONDS = 120

# --- Object API -------------------------------------------------------------------
PATH_TEMPLATE = "/{version}/objects/{object}"
SCHEMA_MEDIA_TYPE = "application/schema+json"
# ``*.*`` adds contact points, custom field groups (flexi-fields) and related
# values.  Custom fields carry RMU-specific data the lineage needs (IPEDS HR
# race/ethnicity, tenure, rank, occupational category), so they are on by default.
DEFAULT_SELECT = "*.*"
COMPANY_KEY = "companyId"
UPDATED_AT_PATH = "lastUpdated/updatedAt"

# --- Change capture -------------------------------------------------------------
DEFAULT_LOOKBACK_HOURS = 1
DEFAULT_RECONCILIATION_INTERVAL_HOURS = 24

KeyKind = Literal["text", "bigint", "date"]
"""How a key value is typed and written into a filter literal.

``text`` is single-quoted, ``bigint`` is bare, ``date`` is a bare ISO-8601
literal stored as text (dlt would otherwise guess a timestamp)."""


@dataclass(frozen=True)
class Key:
    """One field of an object's key.

    Attributes:
        path: Field path as the filter syntax spells it (``a/b`` when nested).
        kind: Literal and column type (see ``KeyKind``).
    """

    path: str
    kind: KeyKind = "text"

    @property
    def column(self) -> str:
        """Top-level column the key is loaded into (``a/b`` → ``a__b``)."""
        return self.path.replace("/", "__")


@dataclass(frozen=True)
class UnitObject:
    """One Object API object loaded as one table.

    Attributes:
        name: Resource / table name (snake_case of the object).
        object: Object segment of the path (``general-ledger-transactions``).
        version: API version segment (``v1`` / ``v2``).
        keys: The object's key fields, ``companyId`` first.
        incremental: ``True`` when the object carries ``lastUpdated``.
        params: Extra fixed query parameters (object-specific flags).
        area: Unit4 reference area, for the README matrix.
        restricted: Loaded only when named in ``resources`` (payroll, rates).
    """

    name: str
    object: str
    version: str
    keys: tuple[Key, ...]
    incremental: bool = True
    params: Mapping[str, Any] = field(default_factory=dict)
    area: str = ""
    restricted: bool = False

    @property
    def path(self) -> str:
        """API path below ``base_url``."""
        return PATH_TEMPLATE.format(version=self.version, object=self.object)


_CO = Key(COMPANY_KEY)


def _obj(
    name: str,
    version: str,
    keys: tuple[Key, ...],
    *,
    area: str,
    incremental: bool = True,
    params: Mapping[str, Any] | None = None,
    restricted: bool = False,
) -> UnitObject:
    return UnitObject(
        name=name,
        object=name.replace("_", "-"),
        version=version,
        keys=(_CO, *keys),
        incremental=incremental,
        params=dict(params or {}),
        area=area,
        restricted=restricted,
    )


# Coverage: the finance and personnel objects the RMU assessment and lineage
# draft need (GL, chart of accounts and dimensions, budgets, AP/AR, projects,
# bank reconciliation, employees and positions), plus their reference objects.
# Other Object API objects are excluded until RMU's tenant is confirmed; the
# README lists why.
OBJECTS: tuple[UnitObject, ...] = (
    # --- General ledger and chart of accounts ---
    _obj(
        "general_ledger_transactions",
        "v2",
        (Key("transactionNumber", "bigint"), Key("additionalInformation/sequenceNumber", "bigint")),
        area="Accounting",
    ),
    _obj("accounts", "v1", (Key("account"), Key("periodFrom", "bigint")), area="Common"),
    _obj("account_groups", "v1", (Key("accountGroup"),), area="Common"),
    _obj(
        "attributes",
        "v1",
        (Key("attributeId"), Key("attributeName")),
        area="Common",
        incremental=False,
    ),
    _obj(
        "attribute_values",
        "v1",
        (Key("attributeId"), Key("attributeValue"), Key("periodFrom", "bigint")),
        area="Common",
    ),
    _obj("periods", "v1", (Key("periodType"), Key("period", "bigint")), area="Common"),
    UnitObject(name="companies", object="companies", version="v1", keys=(_CO,), area="Common"),
    # --- Budgets ---
    _obj(
        "planner_transactions",
        "v1",
        (Key("period", "bigint"), Key("transactionId", "bigint")),
        area="Planner",
    ),
    _obj("planner_versions", "v1", (Key("version"),), area="Planner", incremental=False),
    # --- Payables and receivables ---
    _obj("suppliers", "v1", (Key("supplierId"),), area="Accounting"),
    _obj(
        "supplier_transactions",
        "v2",
        (Key("transactionNumber", "bigint"), Key("sequenceNumber", "bigint")),
        area="Accounting",
    ),
    _obj("customers", "v1", (Key("customerId"),), area="Accounting"),
    _obj(
        "customer_transactions",
        "v1",
        (Key("transactionNumber", "bigint"), Key("sequenceNo", "bigint")),
        area="Accounting",
    ),
    # --- Bank reconciliation ---
    _obj("bank_statements", "v1", (Key("statementId", "bigint"),), area="Accounting"),
    _obj(
        "bank_reconciliation_transactions",
        "v1",
        (
            Key("reconciliationId", "bigint"),
            Key("fixId", "bigint"),
            Key("type"),
            Key("transactionNumber", "bigint"),
            Key("sequenceNumber", "bigint"),
        ),
        area="Accounting",
    ),
    # --- Projects ---
    _obj("projects", "v1", (Key("projectId"),), area="Project management"),
    _obj("work_orders", "v1", (Key("workOrderId"),), area="Project management"),
    _obj(
        "activities",
        "v1",
        (Key("projectId"), Key("activityId")),
        area="Project management",
        incremental=False,
    ),
    # --- Personnel ---
    _obj("employees", "v1", (Key("personId"),), area="Personnel"),
    _obj(
        "employments",
        "v1",
        (Key("personId"), Key("employmentId", "bigint"), Key("positionId")),
        area="Personnel",
        incremental=False,
    ),
    _obj("positions", "v1", (Key("positionId"),), area="Personnel"),
    _obj("position_codes", "v1", (Key("positionCode"),), area="Personnel"),
    _obj("resources", "v1", (Key("personId"),), area="Project management"),
    # --- Restricted (payroll): opt-in by name ---
    _obj(
        "payroll_transactions",
        "v1",
        (
            Key("batchId", "bigint"),
            Key("transactionNumber", "bigint"),
            Key("sequenceNumber", "bigint"),
        ),
        area="Payroll",
        restricted=True,
    ),
    _obj(
        "employee_rates",
        "v1",
        (Key("personId"), Key("positionId"), Key("valueReference"), Key("dateFrom", "date")),
        area="Personnel",
        restricted=True,
    ),
)

OBJECTS_BY_NAME: Mapping[str, UnitObject] = {o.name: o for o in OBJECTS}

FIELD_DEFINITIONS = "field_definitions"

# Standard columns (CONNECTOR_SPEC §10.1).  ``_source_account_id`` is the
# ``companyId`` the row was read with: one credential reaches every company.
SOURCE_ACCOUNT_COLUMN = "_source_account_id"
STANDARD_COLUMNS: Mapping[str, str] = {
    SOURCE_ACCOUNT_COLUMN: "text",
    "_source_created_at": "timestamp",
    "_source_updated_at": "timestamp",
    "_is_deleted": "bool",
    "_deleted_at": "timestamp",
    "_extracted_at": "timestamp",
}
