"""Pipedrive dlt source — 7 resources matching the legacy Airbyte streams.

Public surface mirrors the Airbyte connector: same resource names, same
field shapes after parquet schema inference.

Authentication is Pipedrive **Personal API token** passed in the
``x-api-token`` request header. Pipedrive also accepts the token as a
``?api_token=<value>`` query parameter, but that form leaks the secret into
error logs (``requests`` embeds the full URL in every ``HTTPError``), so we
use the header exclusively. Bearer auth only works on Pipedrive's v2 API with
OAuth access tokens. Each workspace mints one long-lived token from
Settings → Personal preferences → API.

Resources:

- ``users``         — via ``/recents?items=user``; incremental on ``modified``
- ``persons``       — ``/persons``; incremental on ``update_time``
- ``leads``         — ``/leads``; incremental on ``update_time``
- ``organizations`` — ``/organizations``; incremental on ``update_time``
- ``deals``         — ``/deals``; incremental on ``update_time``
- ``activities``    — ``/activities``; incremental on ``update_time``
- ``stages``        — ``/stages``; full ``replace`` each run (no update cursor)
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.client import RESTClient

from .helpers import (
    PipedrivePaginator,
    columns,
    flatten_deal_user_refs,
    flatten_person_contact_arrays,
    flatten_person_owner_ref,
)
from .settings import (
    ACTIVITIES_ALL_USERS_PARAM,
    DEFAULT_PAGE_SIZE,
    DEFAULT_START,
    EPOCH_ISO,
    PIPEDRIVE_API_BASE_URL,
)

Row = dict[str, Any]

# Per-object column hints — declare the schema each downstream consumer
# reads so dlt always materializes the column even when zero rows / all-NULL.
_OBJECT_COLUMNS: dict[str, dict[str, dict[str, Any]]] = {
    "users": columns(
        text=("name", "email", "default_currency", "timezone_name"),
    ),
    "persons": columns(
        text=("name", "first_name", "org_name", "email_primary", "phone_primary"),
        bigint=("owner_id", "org_id"),
    ),
    "leads": columns(
        text=("title", "source_name"),
        bigint=("owner_id", "creator_id", "person_id", "organization_id"),
    ),
    "organizations": columns(
        text=("name", "owner_name"),
    ),
    "deals": columns(
        text=("title", "currency", "status", "person_name", "owner_name"),
        bigint=(
            "stage_id",
            "pipeline_id",
            "person_id",
            "user_id",
            "creator_user_id",
        ),
    ),
    "activities": columns(
        text=("type", "subject", "owner_name", "due_date", "due_time", "duration"),
        bigint=("user_id", "assigned_to_user_id", "company_id"),
    ),
    # `stages` joins into the sales-mart intermediate via
    # (pipeline_id, stage_id). Pin every field the staging model selects so
    # dlt doesn't drop them on a sparse batch — Pipedrive accounts with
    # only a few stages produce small batches where one optional field
    # being NULL on every row would silently drop the column from parquet.
    "stages": columns(
        text=("name", "pipeline_name"),
        bigint=(
            "id",
            "pipeline_id",
            "order_nr",
            # `deal_probability` is `0..100` integer per Pipedrive's API.
            "deal_probability",
            # `rotten_days` may be NULL when rotten_flag is false.
            "rotten_days",
        ),
    ),
}


@dlt.source(name="pipedrive")
def pipedrive_source(
    api_key: str = dlt.secrets.value,
    base_url: str = PIPEDRIVE_API_BASE_URL,
) -> list[Any]:
    """Pipedrive source factory — yields 7 resources.

    Args:
        api_key: Pipedrive Personal API token. Resolved from secrets by default.
            Mint at Pipedrive → Settings → Personal preferences → API.
        base_url: API base URL — override for testing or sandbox.
    """
    # Pipedrive personal tokens go in the `x-api-token` header, NOT the
    # `?api_token=<value>` query param and NOT `Authorization: Bearer …`
    # (Bearer is v2-OAuth only). The header form is mandatory here for
    # security: requests bakes the full `response.url` (query string included)
    # into every `HTTPError` message, dlt re-wraps that in
    # `ResourceExtractionError`, and Dagster logs/stores it — so a query-string
    # token leaks the secret into logs on any 4xx/5xx. A header keeps the token
    # out of the URL entirely.
    auth = APIKeyAuth(name="x-api-token", api_key=api_key, location="header")

    client = RESTClient(
        base_url=base_url,
        auth=auth,
        paginator=PipedrivePaginator(),
        data_selector="data",
    )

    # Extra query params per endpoint. Activities must pass `user_id=0` to
    # return all-company records rather than filtering to the token user.
    _list_extra_params: dict[str, dict[str, int]] = {
        "activities": ACTIVITIES_ALL_USERS_PARAM,
    }

    def _list_resource(object_name: str, cursor_field: str) -> Any:
        @dlt.resource(
            name=object_name,
            primary_key="id",
            write_disposition="append",
            columns=_OBJECT_COLUMNS[object_name],
        )
        def _r(
            cursor: Any = dlt.sources.incremental(  # noqa: B008
                cursor_field,
                initial_value=EPOCH_ISO,
                range_start="open",
            ),
        ) -> Iterator[Row]:
            params: dict[str, Any] = {
                "limit": DEFAULT_PAGE_SIZE,
                "start": DEFAULT_START,
                **_list_extra_params.get(object_name, {}),
            }
            for page in client.paginate(f"/{object_name}", params=params):
                yield from page

        return _r

    persons = _list_resource("persons", cursor_field="update_time")
    # Pipedrive's `/persons` endpoint returns `email`/`phone` as arrays of
    # `{label, value, primary}` objects. dlt would otherwise splay these
    # into child tables (`persons__email`, `persons__phone`); flattening to
    # `email_primary`/`phone_primary` here matches the legacy staging
    # model's `data->'email'->0->>'value'` (first element's value).
    persons = persons.add_map(flatten_person_contact_arrays)
    # `/v1/persons` returns `owner_id` as a nested user object (unlike
    # `org_id` on the same endpoint which is a scalar). Flatten to id.
    persons = persons.add_map(flatten_person_owner_ref)

    # `users` is sourced via `/recents?items=user` (matching the legacy
    # Airbyte connector), not `/users` directly. Pipedrive removed the
    # `modified` field from `/users` for non-admin tokens on 2023-02-09
    # (https://developers.pipedrive.com/changelog/post/removal-of-fields-from-users-api);
    # `/recents` still surfaces it on every record and supports server-side
    # `since_timestamp` filtering. The cursor field stays `modified` so
    # the dbt staging model's `try_cast(modified as timestamp)` keeps working.
    @dlt.resource(
        name="users",
        primary_key="id",
        write_disposition="append",
        columns=_OBJECT_COLUMNS["users"],
    )
    def users(
        cursor: Any = dlt.sources.incremental(  # noqa: B008
            "modified",
            initial_value=EPOCH_ISO,
            range_start="open",
        ),
    ) -> Iterator[Row]:
        # Pipedrive expects `YYYY-MM-DD HH:MM:SS`; EPOCH_ISO is ISO-Zulu
        # and subsequent watermarks come straight from the `modified`
        # field which is already in Pipedrive's format.
        since_ts = cursor.start_value.replace("T", " ").rstrip("Z")[:19]
        params: dict[str, Any] = {
            "limit": DEFAULT_PAGE_SIZE,
            "start": DEFAULT_START,
            "items": "user",
            "since_timestamp": since_ts,
        }
        for page in client.paginate("/recents", params=params):
            for envelope in page:
                if not isinstance(envelope, dict):
                    continue
                inner = envelope.get("data")
                if isinstance(inner, dict):
                    yield inner

    # `stages` is configuration data — small static set per account
    # (typically <20 rows total). No reliable update cursor on the
    # endpoint; `replace` disposition snapshots the full set every run.
    # Powers the sales-pipeline intermediate's funnel-stage mapping via
    # the `opportunity_stage_map` seed (keyed by `pipeline_name + stage_name`).
    @dlt.resource(
        name="stages",
        primary_key="id",
        write_disposition="replace",
        columns=_OBJECT_COLUMNS["stages"],
    )
    def stages() -> Iterator[Row]:
        for page in client.paginate(
            "/stages", params={"limit": DEFAULT_PAGE_SIZE, "start": DEFAULT_START}
        ):
            yield from page

    # Pipedrive's `deals` endpoint returns `user_id` and `creator_user_id`
    # as nested `{id, name, email, ...}` objects rather than scalar ids.
    deals = _list_resource("deals", cursor_field="update_time").add_map(flatten_deal_user_refs)

    return [
        users,
        persons,
        _list_resource("leads", cursor_field="update_time"),
        _list_resource("organizations", cursor_field="update_time"),
        deals,
        _list_resource("activities", cursor_field="update_time"),
        stages,
    ]


__all__ = ["pipedrive_source"]
