"""HubSpot source — endpoint constants and property whitelists."""

from __future__ import annotations

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"

# CRM v3 list endpoint page size
CRM_PAGE_LIMIT = 100

# Engagements v1 page size (max 250)
ENGAGEMENTS_PAGE_LIMIT = 250

# Cursor floor for ISO-timestamp incrementals.  Real updatedAt values are
# always strictly greater, so this is the "load everything" starting point
# on a never-run pipeline.
EPOCH_ISO = "1970-01-01T00:00:00Z"

# Property whitelists per CRM v3 object — HubSpot only returns the listed
# properties (plus a small default set), so any field the staging models
# read must appear here.  Keep aligned with
# `dbt/models/staging/hubspot/stg_hubspot__*.sql`.
COMPANY_PROPERTIES: tuple[str, ...] = (
    "name",
    "domain",
    "website",
    "phone",
    "industry",
    "city",
    "state",
    "country",
    "lifecyclestage",
    "annualrevenue",
    "numberofemployees",
    "num_associated_contacts",
    "num_associated_deals",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
)
CONTACT_PROPERTIES: tuple[str, ...] = (
    "email",
    "firstname",
    "lastname",
    "phone",
    "company",
    "jobtitle",
    "city",
    "state",
    "country",
    "lifecyclestage",
    "associatedcompanyid",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
)
DEAL_PROPERTIES: tuple[str, ...] = (
    "dealname",
    "dealstage",
    "dealtype",
    "pipeline",
    "amount",
    "amount_in_home_currency",
    "deal_currency_code",
    "hubspot_owner_id",
    "closed_lost_reason",
    "closed_won_reason",
    "days_to_close",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
)

CRM_OBJECT_PROPERTIES: dict[str, tuple[str, ...]] = {
    "companies": COMPANY_PROPERTIES,
    "contacts": CONTACT_PROPERTIES,
    "deals": DEAL_PROPERTIES,
}
