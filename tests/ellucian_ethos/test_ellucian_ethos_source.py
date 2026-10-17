"""Integration tests for ellucian_ethos_source against a DuckDB destination.

All Ethos HTTP is mocked with ``responses`` (never pytest-httpx, per CONTRIBUTING.md).
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import responses
from dlt.pipeline.exceptions import PipelineStepFailed

from paradox_dlt_sources.ellucian_ethos import ellucian_ethos_source
from paradox_dlt_sources.ellucian_ethos.settings import RESOURCES

_BASE = "https://integrate.elluciancloud.test"
_AUTH = f"{_BASE}/auth"
_CONSUME = f"{_BASE}/consume"
_PERSONS = f"{_BASE}/api/persons"
_V12 = "application/vnd.hedtech.integration.v12+json"


def _source(resources: list[str] | None = None, **kwargs: Any) -> Any:
    options: dict[str, Any] = {
        "credentials": {"api_key": "key-123"},
        "base_url": _BASE,
        "resources": resources if resources is not None else ["persons"],
        "max_attempts": 1,
        **kwargs,
    }
    return ellucian_ethos_source(**options)


def _person(guid: str, first: str, modified: str | None = None) -> dict[str, Any]:
    person: dict[str, Any] = {
        "id": guid,
        "names": [{"firstName": first, "lastName": "Doe", "preference": "preferred"}],
        "credentials": [{"type": "bannerId", "value": f"B{guid[:4]}"}],
    }
    if modified:
        person["metadata"] = {"modifiedOn": modified}
    return person


def _list_callback(records: list[dict[str, Any]], cap: int | None = None) -> Any:
    """Serve ``records`` with offset/limit paging and Ethos headers."""

    def callback(request: Any) -> tuple[int, dict[str, str], str]:
        query = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        offset, limit = int(query["offset"]), int(query["limit"])
        size = min(limit, cap) if cap else limit
        headers = {"X-Total-Count": str(len(records)), "X-Media-Type": _V12}
        if cap:
            headers["X-Max-Page-Size"] = str(cap)
        return 200, headers, json.dumps(records[offset : offset + size])

    return callback


def _message(
    msg_id: int, guid: str, operation: str, content: Any = None, resource: str = "persons"
) -> dict[str, Any]:
    return {
        "id": msg_id,
        "published": f"2026-10-0{msg_id % 9 + 1}T12:00:00Z",
        "resource": {"name": resource, "id": guid, "version": _V12},
        "operation": operation,
        "contentType": "resource-representation" if content else "empty",
        "content": content,
        "publisher": {"applicationName": "Banner"},
    }


def _register(
    rsps: responses.RequestsMock,
    persons: list[dict[str, Any]] | None = None,
    messages: list[dict[str, Any]] | None = None,
    cap: int | None = None,
    *,
    feed: bool = True,
) -> None:
    rsps.add(responses.POST, _AUTH, body="jwt-token")
    if persons is not None:
        rsps.add_callback(responses.GET, _PERSONS, callback=_list_callback(persons, cap))
    if feed:
        rsps.add(responses.GET, _CONSUME, json=messages or [], headers={"X-Remaining": "0"})


def _query(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


def _params(call: Any) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(call.request.url).query).items()}


def _calls(rsps: responses.RequestsMock, url: str) -> list[Any]:
    return [c for c in rsps.calls if c.request.url.split("?")[0] == url]


_A = "a0000000-0000-0000-0000-000000000001"
_B = "b0000000-0000-0000-0000-000000000002"
_C = "c0000000-0000-0000-0000-000000000003"


# --- full read --------------------------------------------------------------------


def test_full_read_pages_by_offset_and_honours_server_page_cap(tmp_pipeline: Any) -> None:
    persons = [_person(_A, "Ann", "2026-09-01T10:00:00Z"), _person(_B, "Bob"), _person(_C, "Cy")]
    with responses.RequestsMock() as rsps:
        _register(rsps, persons, cap=2)
        tmp_pipeline.run(_source(page_size=500))
        pages = [_params(c) for c in _calls(rsps, _PERSONS)]
        auth = _calls(rsps, _AUTH)[0]

    assert [(p["offset"], p["limit"]) for p in pages] == [("0", "500"), ("2", "2")]
    assert auth.request.headers["Authorization"] == "Bearer key-123"
    rows = _query(
        tmp_pipeline,
        "select id, _is_deleted, _ethos_media_type, _source_updated_at from persons order by id",
    )
    assert [r[0] for r in rows] == [_A, _B, _C]
    assert all(r[1] is False and r[2] == _V12 for r in rows)
    assert rows[0][3] is not None and rows[1][3] is None


def test_requests_carry_the_access_token_and_pinned_version(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann")])
        tmp_pipeline.run(_source(versions={"persons": "12"}))
        call = _calls(rsps, _PERSONS)[0]

    assert call.request.headers["Authorization"] == "Bearer jwt-token"
    assert call.request.headers["Accept"] == _V12


def test_full_read_tombstones_missing_ids_and_keeps_their_data(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann"), _person(_B, "Bob")], feed=False)
        tmp_pipeline.run(_source(change_feed=False))
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann")], feed=False)
        tmp_pipeline.run(_source(change_feed=False))

    rows = _query(
        tmp_pipeline, "select id, _is_deleted, _deleted_at, names from persons order by id"
    )
    assert [(r[0], r[1]) for r in rows] == [(_A, False), (_B, True)]
    assert rows[1][2] is not None
    assert "Bob" in str(rows[1][3])


def test_incomplete_walk_skips_reconciliation(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann"), _person(_B, "Bob")], feed=False)
        tmp_pipeline.run(_source(change_feed=False))

    def short(request: Any) -> tuple[int, dict[str, str], str]:
        # Claims 2 rows, then serves an empty second page.
        offset = int(parse_qs(urlparse(request.url).query)["offset"][0])
        body = [_person(_A, "Ann")] if offset == 0 else []
        return 200, {"X-Total-Count": "2"}, json.dumps(body)

    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, _AUTH, body="jwt-token")
        rsps.add_callback(responses.GET, _PERSONS, callback=short)
        tmp_pipeline.run(_source(change_feed=False))

    rows = _query(tmp_pipeline, "select id, _is_deleted from persons order by id")
    assert rows == [(_A, False), (_B, False)]


def test_empty_read_against_loaded_table_refuses_to_delete_everything(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann")], feed=False)
        tmp_pipeline.run(_source(change_feed=False))
    with responses.RequestsMock() as rsps, pytest.raises(PipelineStepFailed):
        _register(rsps, [], feed=False)
        tmp_pipeline.run(_source(change_feed=False))


# --- change feed ------------------------------------------------------------------


def test_feed_applies_latest_change_per_id_and_tombstones_deletes(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann"), _person(_B, "Bob")])
        tmp_pipeline.run(_source())
        first_consume = _params(_calls(rsps, _CONSUME)[0])

    messages = [
        _message(11, _A, "replaced", _person(_A, "Anna")),
        _message(12, _A, "replaced", _person(_A, "Annie")),
        _message(13, _B, "deleted"),
        _message(14, _C, "created", _person(_C, "Cy")),
        _message(15, "d0000000-0000-0000-0000-000000000004", "created", {"id": "x"}, "courses"),
    ]
    with responses.RequestsMock() as rsps:
        _register(rsps, messages=messages)
        tmp_pipeline.run(_source())
        assert not _calls(rsps, _PERSONS)
    with responses.RequestsMock() as rsps:
        _register(rsps)
        tmp_pipeline.run(_source())
        third_consume = _params(_calls(rsps, _CONSUME)[0])

    assert "lastProcessedID" not in first_consume
    assert third_consume["lastProcessedID"] == "15"
    rows = _query(tmp_pipeline, "select id, _is_deleted, names from persons order by id")
    assert [(r[0], r[1]) for r in rows] == [(_A, False), (_B, True), (_C, False)]
    assert "Annie" in str(rows[0][2])
    assert "Bob" in str(rows[1][2])

    versions = _query(
        tmp_pipeline,
        "select id, _change_op, _change_position from persons_versions order by _change_position",
    )
    assert [(v[0], v[1]) for v in versions] == [
        (_A, "update"),
        (_A, "update"),
        (_B, "delete"),
        (_C, "insert"),
    ]
    assert versions[0][2] == "00000000000000000011"
    logged = _query(tmp_pipeline, "select id from change_notifications order by id")
    assert [r[0] for r in logged] == ["11", "12", "13", "14", "15"]


def test_feed_fetches_records_sent_without_content_once(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann")])
        tmp_pipeline.run(_source())
    with responses.RequestsMock() as rsps:
        _register(rsps, messages=[_message(21, _A, "replaced")])
        rsps.add(responses.GET, f"{_PERSONS}/{_A}", json=_person(_A, "Ann B"))
        tmp_pipeline.run(_source())
        fetches = _calls(rsps, f"{_PERSONS}/{_A}")

    assert len(fetches) == 1
    assert fetches[0].request.headers["Accept"] == _V12
    rows = _query(tmp_pipeline, "select names from persons")
    assert "Ann B" in str(rows[0][0])


def test_resync_reads_in_full_even_with_feed(tmp_pipeline: Any) -> None:
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann")])
        tmp_pipeline.run(_source())
    with responses.RequestsMock() as rsps:
        _register(rsps, [_person(_A, "Ann"), _person(_B, "Bob")])
        tmp_pipeline.run(_source(resync=True))

    rows = _query(tmp_pipeline, "select id from persons order by id")
    assert [r[0] for r in rows] == [_A, _B]


# --- resource selection -----------------------------------------------------------


def test_default_selection_excludes_restricted_resources() -> None:
    source = _source(resources=None)
    names = set(source.resources.keys())
    assert "persons" in names and "persons_versions" in names
    assert "change_notifications" in names
    assert "financial_aid_applications" not in names
    restricted = [r.table for r in RESOURCES if r.restricted]
    assert restricted and not set(restricted) & names


def test_unknown_resource_is_rejected_unless_declared_extra() -> None:
    with pytest.raises(ValueError, match="unknown resource"):
        _source(resources=["rmu-custom-thing"])
    source = _source(resources=["rmu-custom-thing"], extra_resources=["rmu-custom-thing"])
    assert "rmu_custom_thing" in source.resources


def test_missing_api_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="api_key"):
        ellucian_ethos_source(credentials={}, base_url=_BASE, resources=["persons"])
