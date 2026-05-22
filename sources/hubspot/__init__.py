"""HubSpot dlt source — 5 resources matching the legacy Airbyte streams.

Public surface mirrors the Airbyte connector: same resource names, same
field shapes after parquet schema inference.

Authentication is HubSpot **Private App** bearer-token (per-BU).  Each
BU's HubSpot Private App is installed once and emits a long-lived access
token; that token is the only secret this source needs.  OAuth was a
3-secret detour added by an earlier PAR-116 iteration and is no longer
required.

Endpoints:

- ``companies`` / ``contacts`` / ``deals`` use the CRM v3 list endpoint
  with an explicit ``properties=`` whitelist matching what the staging
  models read (HubSpot only returns the listed properties).
- ``engagements`` uses the legacy v1 paged endpoint — CRM v3 split
  engagements into per-type objects (calls/emails/meetings/notes/tasks);
  the v1 endpoint preserves the unified shape the staging model expects.
- ``deal_pipelines`` uses CRM v3 pipelines (single page; small static
  set); full-refresh ``replace`` rather than incremental.

Pagination:

- CRM v3: cursor at ``paging.next.after``, set as ``?after=``.
- Engagements v1: server-supplied ``offset`` field with ``hasMore`` flag.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator

from .helpers import EngagementsOffsetPaginator, columns
from .settings import (
    COMPANY_PROPERTIES,
    CONTACT_PROPERTIES,
    CRM_OBJECT_PROPERTIES,
    CRM_PAGE_LIMIT,
    DEAL_PROPERTIES,
    ENGAGEMENTS_PAGE_LIMIT,
    EPOCH_ISO,
    HUBSPOT_API_BASE_URL,
)

Row = dict[str, Any]


def _crm_object_columns(text_props: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """CRM v3 returns properties under ``properties.<name>`` which dlt
    flattens to ``properties__<name>`` — hint each as nullable text."""
    return columns(text=tuple(f"properties__{p}" for p in text_props))


_OBJECT_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "companies": _crm_object_columns(COMPANY_PROPERTIES),
    "contacts": _crm_object_columns(CONTACT_PROPERTIES),
    "deals": _crm_object_columns(DEAL_PROPERTIES),
    "engagements": columns(
        text=(
            "body_preview",
            "associations__contact_ids",
            "associations__company_ids",
            "associations__deal_ids",
        )
    ),
    "deal_pipelines": columns(text=("stages",)),
}


def _flatten_engagement(item: Row) -> Row:
    """v1 endpoint returns ``{engagement, associations, ...}``; lift
    engagement fields to the top level so they line up with the legacy
    JSON path the staging model walks (``data->>'id'``,
    ``data->>'timestamp'``, ``data->'associations'->'contactIds'`` ...).

    Association ID lists serialize to JSON-string columns rather than
    splaying into dlt child tables — matches the staging model's
    ``data->'associations'->'contactIds'`` shape (a JSON array).
    """
    engagement: dict[str, Any] = item.get("engagement") or {}
    associations: dict[str, Any] = item.get("associations") or {}
    return {
        **engagement,
        "associations__contact_ids": json.dumps(associations.get("contactIds", [])),
        "associations__company_ids": json.dumps(associations.get("companyIds", [])),
        "associations__deal_ids": json.dumps(associations.get("dealIds", [])),
    }


@dlt.source(name="hubspot")
def hubspot_source(
    api_key: str = dlt.secrets.value,
    base_url: str = HUBSPOT_API_BASE_URL,
) -> list[Any]:
    """HubSpot source factory — yields companies, contacts, deals,
    engagements, and deal_pipelines.

    Args:
        api_key: HubSpot Private App access token (Bearer auth).
                 Resolved from secrets by default.
        base_url: API base URL — override for testing.
    """
    auth = BearerTokenAuth(api_key)

    crm_client = RESTClient(
        base_url=base_url,
        auth=auth,
        paginator=JSONResponseCursorPaginator(
            cursor_path="paging.next.after",
            cursor_param="after",
        ),
    )
    engagements_client = RESTClient(
        base_url=base_url,
        auth=auth,
        paginator=EngagementsOffsetPaginator(),
    )

    def _crm_object_resource(object_name: str) -> Any:
        @dlt.resource(
            name=object_name,
            primary_key="id",
            write_disposition="append",
            columns=_OBJECT_COLUMNS[object_name],
        )
        def _r(
            cursor: Any = dlt.sources.incremental(  # noqa: B008
                # Top-level ``updatedAt`` is set by HubSpot on every CRM v3
                # record and always present.  ``properties.hs_lastmodifieddate``
                # can be null for never-modified records — that null tripped
                # dlt's IncrementalCursorPathHasValueNone in a staging run.
                # Airbyte's manifest uses ``updatedAt`` for the same reason.
                "updatedAt",
                initial_value=EPOCH_ISO,
                range_start="open",
            ),
        ) -> Iterator[Row]:
            params = {
                "limit": CRM_PAGE_LIMIT,
                "properties": ",".join(CRM_OBJECT_PROPERTIES[object_name]),
                "archived": "false",
            }
            for page in crm_client.paginate(
                f"/crm/v3/objects/{object_name}",
                params=params,
                data_selector="results",
            ):
                yield from page

        return _r

    @dlt.resource(
        name="engagements",
        primary_key="id",
        write_disposition="append",
        columns=_OBJECT_COLUMNS["engagements"],
    )
    def engagements(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "lastUpdated",
            initial_value=0,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        # Legacy v1 endpoint preserves the unified engagement shape (one
        # row per call/email/meeting/note/task with a ``type`` discriminator
        # column) that the staging model relies on.
        for page in engagements_client.paginate(
            "/engagements/v1/engagements/paged",
            params={"limit": ENGAGEMENTS_PAGE_LIMIT},
            data_selector="results",
        ):
            for item in page:
                yield _flatten_engagement(item)

    @dlt.resource(
        name="deal_pipelines",
        primary_key="id",
        write_disposition="replace",
        columns=_OBJECT_COLUMNS["deal_pipelines"],
    )
    def deal_pipelines() -> Iterator[Row]:
        # ``/crm/v3/pipelines/deals`` returns all pipelines on a single
        # page; tiny set, no incremental needed.
        body: dict[str, Any] = crm_client.get("/crm/v3/pipelines/deals").json()
        for pipeline in body.get("results", []):
            # ``stages`` is an array-of-objects; serialize so it lands as a
            # JSON string column rather than a dlt child table — matches
            # the staging model's ``data->'stages'`` (raw JSON) shape.
            stages = pipeline.get("stages", [])
            yield {**pipeline, "stages": json.dumps(stages)}

    return [
        _crm_object_resource("companies")(),
        _crm_object_resource("contacts")(),
        _crm_object_resource("deals")(),
        engagements,
        deal_pipelines,
    ]


__all__ = ["hubspot_source"]
