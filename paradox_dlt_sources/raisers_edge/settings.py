"""Raiser's Edge NXT (Blackbaud SKY API) source: endpoints, resources and conformance."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

API_BASE_URL = "https://api.sky.blackbaud.com"
TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"

# Max page size on every list endpoint used here (events allow 10,000; 5,000 is the common cap).
PAGE_LIMIT = 5000
# /customfield/customfieldcategories caps at 200.
CUSTOM_FIELD_CATEGORY_LIMIT = 200

# `last_modified` floor for a full read. Without a `last_modified` filter SKY API returns no
# `sort_token`, and a paged read can skip records that change mid-read.
FULL_READ_FLOOR = "0001-01-01T00:00:00Z"

DEFAULT_LOOKBACK = timedelta(hours=1)
DEFAULT_RECONCILIATION_INTERVAL = timedelta(days=1)
DEFAULT_MAX_DELETE_SHARE = 0.5


@dataclass(frozen=True)
class ListEndpoint:
    """A SKY API list-all endpoint read with a server-side `last_modified` cursor.

    Attributes:
        resource: dlt resource and table name.
        path: Path under `API_BASE_URL`.
        params: Extra query parameters sent on every request.
        date_fields: Date-time fields that are date-only in RE; loaded as `date`.
        text_columns: Columns declared as text up front (keys, FKs, core fields).
        decimal_columns: Money columns declared as decimal.
        paging: `link` (`next_link` + `sort_token`) or `offset` (`count` + offset).
        money: Whether rows carry money and get the derived `currency_code`.
    """

    resource: str
    path: str
    params: dict[str, Any] = field(default_factory=dict)
    date_fields: tuple[str, ...] = ()
    text_columns: tuple[str, ...] = ()
    decimal_columns: tuple[str, ...] = ()
    paging: str = "link"
    money: bool = False


ENTITY_ENDPOINTS: tuple[ListEndpoint, ...] = (
    ListEndpoint(
        "constituents",
        "/constituent/v1/constituents",
        {"include_inactive": "true", "include_deceased": "true"},
        text_columns=(
            "lookup_id",
            "type",
            "name",
            "first",
            "last",
            "preferred_name",
            "former_name",
            "gender",
        ),
    ),
    ListEndpoint(
        "constituent_addresses",
        "/constituent/v1/addresses",
        {"include_inactive": "true"},
        date_fields=("start", "end"),
        text_columns=("constituent_id", "type", "address_lines", "city", "state", "postal_code"),
    ),
    ListEndpoint(
        "constituent_email_addresses",
        "/constituent/v1/emailaddresses",
        {"include_inactive": "true"},
        text_columns=("constituent_id", "type", "address"),
    ),
    ListEndpoint(
        "constituent_phones",
        "/constituent/v1/phones",
        {"include_inactive": "true"},
        text_columns=("constituent_id", "type", "number"),
    ),
    ListEndpoint(
        "constituent_online_presences",
        "/constituent/v1/onlinepresences",
        {"include_inactive": "true"},
        text_columns=("constituent_id", "type", "address"),
    ),
    ListEndpoint(
        "constituent_relationships",
        "/constituent/v1/relationships",
        text_columns=("constituent_id", "relation_id", "reciprocal_relationship_id", "type"),
    ),
    ListEndpoint(
        "constituent_educations",
        "/constituent/v1/educations",
        text_columns=("constituent_id", "school", "degree", "class_of", "registration_number"),
    ),
    ListEndpoint(
        "constituent_codes",
        "/constituent/v1/constituents/constituentcodes",
        {"include_inactive": "true"},
        text_columns=("constituent_id", "description"),
    ),
    ListEndpoint(
        "constituent_custom_fields",
        "/constituent/v1/constituents/customfields",
        date_fields=("date",),
        text_columns=("parent_id", "category", "type", "comment"),
    ),
    ListEndpoint(
        "constituent_notes",
        "/constituent/v1/notes",
        text_columns=("constituent_id", "type", "summary"),
    ),
    ListEndpoint(
        "actions",
        "/constituent/v1/actions",
        date_fields=("date",),
        text_columns=(
            "constituent_id",
            "opportunity_id",
            "campaign_id",
            "fund_id",
            "appeal_id",
            "category",
            "type",
            "status",
        ),
    ),
    ListEndpoint(
        "memberships",
        "/constituent/v1/memberships",
        date_fields=("joined", "expires"),
        text_columns=("constituent_id", "category", "program", "standing"),
        decimal_columns=("dues__value",),
        money=True,
    ),
    ListEndpoint(
        "gifts",
        "/gift/v1/gifts",
        date_fields=("date", "post_date"),
        text_columns=(
            "constituent_id",
            "lookup_id",
            "type",
            "subtype",
            "post_status",
            "batch_number",
            "gift_status",
        ),
        decimal_columns=("amount__value", "balance__value"),
        money=True,
    ),
    ListEndpoint(
        "gift_custom_fields",
        "/gift/v1/gifts/customfields",
        date_fields=("date",),
        text_columns=("parent_id", "category", "type", "comment"),
    ),
    ListEndpoint(
        "campaigns",
        "/fundraising/v1/campaigns",
        {"include_inactive": "true"},
        text_columns=("lookup_id", "description", "category"),
    ),
    ListEndpoint(
        "funds",
        "/fundraising/v1/funds",
        {"include_inactive": "true"},
        text_columns=("lookup_id", "description", "category", "type"),
    ),
    ListEndpoint(
        "appeals",
        "/fundraising/v1/appeals",
        {"include_inactive": "true"},
        text_columns=("lookup_id", "description", "category"),
    ),
    ListEndpoint(
        "packages",
        "/fundraising/v1/packages",
        {"include_inactive": "true"},
        text_columns=("lookup_id", "description", "appeal_id"),
    ),
    ListEndpoint(
        "opportunities",
        "/opportunity/v1/opportunities",
        {"include_inactive": "true"},
        text_columns=(
            "constituent_id",
            "campaign_id",
            "fund_id",
            "appeal_id",
            "name",
            "status",
            "purpose",
        ),
    ),
    ListEndpoint(
        "events",
        "/event/v1/eventlist",
        {"include_inactive": "true", "sort": "date_modified"},
        text_columns=("lookup_id", "name", "category__id", "group__id"),
        paging="offset",
    ),
)

# Children emitted from their parent's record (§7.4). Key is (parent key, child id).
GIFT_CHILDREN: tuple[tuple[str, str], ...] = (
    ("gift_splits", "gift_splits"),
    ("gift_soft_credits", "soft_credits"),
)

EVENT_PARTICIPANTS_PATH = "/event/v1/events/{event_id}/participants"
EVENT_LIST_PATH = "/event/v1/eventlist"
GIFT_BATCHES_PATH = "/gift-batch/v1/giftbatches"
LISTS_PATH = "/list/v1/lists"
LIST_TYPES = ("Constituent", "Gift", "Action", "Opportunity")
CUSTOM_FIELD_CATEGORIES_PATH = "/customfield/customfieldcategories"
CUSTOM_FIELD_RECORD_TYPES = (
    "Constituent",
    "Gift",
    "Action",
    "Education",
    "Event",
    "Participant",
    "Campaign",
    "Appeal",
    "Fund",
    "Package",
    "Address",
    "Membership",
)
CODE_TABLES_PATH = "/nxt-data-integration/v1/re/codetables"
CODE_TABLE_ENTRIES_PATH = "/nxt-data-integration/v1/re/codetables/{code_table_id}/tableentries"
CURRENCY_CONFIGURATION_PATH = "/constituent/v1/currencyconfiguration"


def _entity(
    change: str = "cursor:last_modified", delete: str = "key_reconciliation"
) -> dict[str, Any]:
    return {
        "class": "entity",
        "level": "L2",
        "disposition": "upsert",
        "key": ("id",),
        "change": change,
        "delete": delete,
    }


CONFORMANCE: dict[str, Any] = {
    "source_type": 1,
    "profile": "P1",
    "level": "L2",
    "resources": {
        **{e.resource: _entity() for e in ENTITY_ENDPOINTS},
        "gift_splits": {
            "class": "child",
            "level": "L2",
            "disposition": "upsert",
            "key": ("gift_id", "id"),
            "change": "parent:gifts",
            "delete": "parent_version",
        },
        "gift_soft_credits": {
            "class": "child",
            "level": "L2",
            "disposition": "upsert",
            "key": ("gift_id", "id"),
            "change": "parent:gifts",
            "delete": "parent_version",
        },
        "event_participants": _entity(change="cursor:last_modified per event"),
        "gift_batches": _entity(change="full_read"),
        "lists": _entity(change="full_read"),
        "field_definitions": {
            "class": "metadata",
            "level": "L2",
            "disposition": "upsert",
            "key": ("object_type", "field_key"),
            "change": "full_read",
            "delete": "key_reconciliation",
        },
        "field_options": {
            "class": "metadata",
            "level": "L2",
            "disposition": "upsert",
            "key": ("object_type", "field_key", "value"),
            "change": "full_read",
            "delete": "key_reconciliation",
        },
        "currency_configuration": {
            "class": "reference",
            "level": "L2",
            "disposition": "upsert",
            "key": ("currency_code",),
            "change": "full_read",
            "delete": "key_reconciliation",
        },
    },
}
