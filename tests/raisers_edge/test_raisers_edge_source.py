"""Integration tests for raisers_edge_source against a fake SKY API and duckdb."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
import responses
from dlt.pipeline.exceptions import PipelineStepFailed

from paradox_dlt_sources.raisers_edge import raisers_edge_source
from paradox_dlt_sources.raisers_edge.helpers import SkyQuotaExceededError
from paradox_dlt_sources.raisers_edge.settings import ENTITY_ENDPOINTS, FULL_READ_FLOOR
from tests.raisers_edge.fake_sky import FakeSky, pages

CONSTITUENTS = "/constituent/v1/constituents"
GIFTS = "/gift/v1/gifts"


def _constituent(cid: str, modified: str, name: str = "") -> dict[str, Any]:
    return {
        "id": cid,
        "lookup_id": f"L{cid}",
        "name": name or f"Person {cid}",
        "last": "Doe",
        "type": "Individual",
        "inactive": False,
        "deceased": False,
        "date_added": "2020-01-01T10:00:00-05:00",
        "date_modified": modified,
        "email": {"address": f"p{cid}@example.edu", "primary": True},
    }


def _gift(gid: str, modified: str, splits: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": gid,
        "constituent_id": "1",
        "amount": {"value": 150.25},
        "date": "2026-09-30T00:00:00",
        "post_date": "2026-10-01T00:00:00",
        "post_status": "Posted",
        "type": "Donation",
        "date_added": "2026-09-30T09:00:00-04:00",
        "date_modified": modified,
        "gift_splits": splits,
        "soft_credits": [{"id": "sc1", "constituent_id": "2", "amount": {"value": 150.25}}],
    }


def _source(**kwargs: Any) -> Any:
    return raisers_edge_source(
        client_id="cid",
        client_secret="csec",
        refresh_token="rtoken",
        subscription_key="subkey",
        source_timezone="America/New_York",
        **kwargs,
    )


def _rows(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


@responses.activate
def test_first_run_full_read(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[CONSTITUENTS] = pages(
        [_constituent("1", "2026-10-01T12:00:00-04:00")],
        [_constituent("2", "2026-10-02T12:00:00-04:00")],
        path=CONSTITUENTS,
    )
    sky.routes[GIFTS] = pages(
        [
            _gift(
                "g1",
                "2026-10-01T12:00:00-04:00",
                [
                    {"id": "s1", "amount": {"value": 100.25}, "fund_id": "f1"},
                    {"id": "s2", "amount": {"value": 50}, "fund_id": "f2"},
                ],
            )
        ],
        path=GIFTS,
    )

    info = tmp_pipeline.run(_source())
    assert not info.has_failed_jobs

    first = sky.params_for(CONSTITUENTS)[0]
    assert first["include_inactive"] == ["true"]
    assert first["include_deceased"] == ["true"]
    assert first["last_modified"] == [FULL_READ_FLOOR]
    assert first["limit"] == ["5000"]
    assert sky.params_for(CONSTITUENTS)[1]["sort_token"] == ["1"]

    tables = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {
        "constituents",
        "gifts",
        "gift_splits",
        "gift_soft_credits",
        "currency_configuration",
    } <= tables

    rows = _rows(
        tmp_pipeline,
        "SELECT id, _is_deleted, _source_updated_at, _extracted_at, email__address "
        "FROM constituents ORDER BY id",
    )
    assert [(r[0], r[1], r[4]) for r in rows] == [
        ("1", False, "p1@example.edu"),
        ("2", False, "p2@example.edu"),
    ]
    assert all(r[2] is not None and r[3] is not None for r in rows)

    gift = _rows(tmp_pipeline, "SELECT date, post_date, amount__value, currency_code FROM gifts")[0]
    assert str(gift[0]) == "2026-09-30"
    assert str(gift[1]) == "2026-10-01"
    assert str(gift[2]).startswith("150.25")
    assert gift[3] == "USD"

    splits = _rows(
        tmp_pipeline,
        "SELECT gift_id, id, fund_id, _parent_version FROM gift_splits ORDER BY id",
    )
    assert [(s[0], s[1], s[2]) for s in splits] == [("g1", "s1", "f1"), ("g1", "s2", "f2")]
    assert len({s[3] for s in splits}) == 1

    for ep in ENTITY_ENDPOINTS:
        if ep.resource not in tables:
            continue
        cols = tmp_pipeline.default_schema.get_table_columns(ep.resource)
        assert {"_is_deleted", "_deleted_at", "_extracted_at", "_source_updated_at"} <= set(cols)


@responses.activate
def test_second_run_reads_from_cursor_minus_lookback(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[CONSTITUENTS] = pages(
        [_constituent("1", "2026-10-01T12:00:00-04:00")], path=CONSTITUENTS
    )
    tmp_pipeline.run(_source(resources=["constituents"]))
    sky.routes[CONSTITUENTS] = pages(
        [_constituent("1", "2026-10-03T08:00:00-04:00", name="Renamed")], path=CONSTITUENTS
    )
    tmp_pipeline.run(_source(resources=["constituents"], lookback=timedelta(minutes=30)))

    assert sky.params_for(CONSTITUENTS)[-1]["last_modified"] == ["2026-10-01T15:30:00+00:00"]
    assert _rows(tmp_pipeline, "SELECT id, name FROM constituents") == [("1", "Renamed")]


@responses.activate
def test_reconciliation_tombstones_and_restores(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    both = [
        _constituent("1", "2026-10-01T12:00:00-04:00"),
        _constituent("2", "2026-10-01T13:00:00-04:00"),
        _constituent("3", "2026-10-01T14:00:00-04:00"),
    ]
    sky.routes[CONSTITUENTS] = pages(both, path=CONSTITUENTS)
    run = {"resources": ["constituents"], "delete_reconciliation_interval": timedelta(0)}
    tmp_pipeline.run(_source(**run))

    sky.routes[CONSTITUENTS] = pages([both[0], both[2]], path=CONSTITUENTS)
    tmp_pipeline.run(_source(**run))
    assert sky.params_for(CONSTITUENTS)[-1]["last_modified"] == [FULL_READ_FLOOR]
    rows = _rows(
        tmp_pipeline,
        "SELECT id, _is_deleted, _deleted_at IS NOT NULL, name, email__address "
        "FROM constituents ORDER BY id",
    )
    assert rows[1] == ("2", True, True, "Person 2", "p2@example.edu")
    assert rows[0][1] is False and rows[2][1] is False

    sky.routes[CONSTITUENTS] = pages(both, path=CONSTITUENTS)
    tmp_pipeline.run(_source(**run))
    assert _rows(tmp_pipeline, "SELECT _is_deleted FROM constituents WHERE id = '2'") == [(False,)]


@responses.activate
def test_reconciliation_refuses_mass_delete(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[CONSTITUENTS] = pages(
        [
            _constituent("1", "2026-10-01T12:00:00-04:00"),
            _constituent("2", "2026-10-01T13:00:00-04:00"),
        ],
        path=CONSTITUENTS,
    )
    run = {"resources": ["constituents"], "delete_reconciliation_interval": timedelta(0)}
    tmp_pipeline.run(_source(**run))
    sky.routes[CONSTITUENTS] = {"count": 0, "value": []}
    with pytest.raises(PipelineStepFailed, match="max_delete_share"):
        tmp_pipeline.run(_source(**run))


@responses.activate
def test_gift_split_removal_leaves_old_parent_version(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[GIFTS] = pages(
        [
            _gift(
                "g1",
                "2026-10-01T12:00:00-04:00",
                [
                    {"id": "s1", "amount": {"value": 100}, "fund_id": "f1"},
                    {"id": "s2", "amount": {"value": 50}, "fund_id": "f2"},
                ],
            )
        ],
        path=GIFTS,
    )
    selected = {"resources": ["gifts", "gift_splits"]}
    tmp_pipeline.run(_source(**selected))
    sky.routes[GIFTS] = pages(
        [
            _gift(
                "g1",
                "2026-10-02T12:00:00-04:00",
                [
                    {"id": "s1", "amount": {"value": 150}, "fund_id": "f1"},
                ],
            )
        ],
        path=GIFTS,
    )
    tmp_pipeline.run(_source(**selected))

    current = _rows(
        tmp_pipeline,
        "SELECT s.id FROM gift_splits s JOIN gifts g ON g.id = s.gift_id "
        "WHERE s._parent_version = strftime(g._source_updated_at AT TIME ZONE 'UTC', "
        "'%Y-%m-%dT%H:%M:%S') || '+00:00'",
    )
    assert current == [("s1",)]
    assert len(_rows(tmp_pipeline, "SELECT id FROM gift_splits")) == 2


@responses.activate
def test_duplicate_key_in_one_read_loads_once(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[CONSTITUENTS] = pages(
        [
            _constituent("1", "2026-10-01T12:00:00-04:00"),
            _constituent("2", "2026-10-01T13:00:00-04:00"),
        ],
        [_constituent("1", "2026-10-01T12:30:00-04:00", name="Changed mid-read")],
        path=CONSTITUENTS,
    )
    info = tmp_pipeline.run(_source(resources=["constituents"]))
    assert not info.has_failed_jobs
    assert _rows(tmp_pipeline, "SELECT count(*) FROM constituents") == [(2,)]
    state = tmp_pipeline.state["sources"]["raisers_edge"]["resources"]["constituents"]
    assert state["cursor"] == "2026-10-01T16:30:00+00:00"


@responses.activate
def test_event_participants_walk_every_event(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes["/event/v1/eventlist"] = {"count": 2, "value": [{"id": "e1"}, {"id": "e2"}]}
    sky.routes["/event/v1/events/e1/participants"] = {
        "count": 1,
        "value": [
            {
                "id": "p1",
                "constituent_id": "1",
                "attended": True,
                "date_added": "2026-01-01T10:00:00-05:00",
                "date_modified": "2026-01-02T10:00:00-05:00",
            }
        ],
    }
    tmp_pipeline.run(_source(resources=["event_participants"]))
    assert sky.params_for("/event/v1/events/e2/participants")
    assert _rows(tmp_pipeline, "SELECT id, event_id, attended FROM event_participants") == [
        ("p1", "e1", True)
    ]


@responses.activate
def test_metadata_resources(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)

    def categories(params: dict[str, list[str]]) -> dict[str, Any]:
        if params["record_type"] == ["Constituent"]:
            return {
                "count": 1,
                "custom_field_categories": [
                    {
                        "custom_field_category_id": 7,
                        "description": "Banner ID",
                        "type_of_data": "Text",
                        "inactive": False,
                    }
                ],
            }
        return {"count": 0, "custom_field_categories": []}

    sky.routes["/customfield/customfieldcategories"] = categories
    sky.routes["/nxt-data-integration/v1/re/codetables"] = {
        "count": 1,
        "value": [{"code_table_id": 3, "name": "Constituent Codes"}],
    }
    sky.routes["/nxt-data-integration/v1/re/codetables/3/tableentries"] = {
        "count": 2,
        "value": [
            {"table_entries_id": 1, "long_description": "Alumni", "sequence": 1, "inactive": False},
            {"table_entries_id": 2, "long_description": "Parent", "sequence": 2, "inactive": True},
        ],
    }
    tmp_pipeline.run(_source(resources=["field_definitions", "field_options"]))

    assert _rows(
        tmp_pipeline, "SELECT object_type, field_key, data_type, is_custom FROM field_definitions"
    ) == [("constituent", "Banner ID", "Text", True)]
    assert _rows(
        tmp_pipeline,
        "SELECT field_key, value, display_order, is_archived FROM field_options ORDER BY value",
    ) == [("Constituent Codes", "Alumni", 1, False), ("Constituent Codes", "Parent", 2, True)]


def test_unknown_resource_raises() -> None:
    with pytest.raises(ValueError, match="nope"):
        _source(resources=["nope"])


@responses.activate
def test_quota_exhaustion_fails_loudly(tmp_pipeline: Any) -> None:
    sky = FakeSky(responses.mock)
    sky.routes[CONSTITUENTS] = (
        403,
        {
            "statusCode": 403,
            "message": "Out of call volume quota. Quota will be replenished in 05:00:00.",
        },
    )
    with pytest.raises(PipelineStepFailed) as exc:
        tmp_pipeline.run(_source(resources=["constituents"]))
    assert isinstance(exc.value.__cause__, SkyQuotaExceededError) or "quota" in str(exc.value)
