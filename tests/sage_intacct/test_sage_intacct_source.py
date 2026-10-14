"""Integration tests for sage_intacct_source against a DuckDB destination.

All Sage Intacct HTTP is mocked with ``responses``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import responses

from paradox_dlt_sources.sage_intacct import sage_intacct_source
from paradox_dlt_sources.sage_intacct.helpers import column_name
from paradox_dlt_sources.sage_intacct.settings import OBJECTS, OBJECTS_BY_RESOURCE
from tests.sage_intacct.fake_api import BASE_URL, FakeSageApi, gl_line

GL_LINES = OBJECTS_BY_RESOURCE["journal_entry_lines"]
DEPARTMENTS = OBJECTS_BY_RESOURCE["departments"]


def _source(entity_ids: list[str] | None = None, **kwargs: Any) -> Any:
    return sage_intacct_source(
        company_id="interstateac",
        user_id="ws_user",
        client_id="cid",
        client_secret="csecret",
        entity_ids=entity_ids,
        base_url=BASE_URL,
        **kwargs,
    )


def _rows(pipeline: Any, sql: str) -> list[tuple[Any, ...]]:
    with pipeline.sql_client() as client:
        return list(client.execute_sql(sql))


@responses.activate
def test_two_entities_tag_rows_with_distinct_entity_id(tmp_pipeline: Any) -> None:
    api = FakeSageApi(
        {
            ("Interstate", GL_LINES.object): [gl_line(1), gl_line(2)],
            ("Parent", GL_LINES.object): [gl_line(1, amount="7.25")],
        }
    )
    api.register(responses.mock)

    tmp_pipeline.run(_source(["Interstate", "Parent"]).with_resources("journal_entry_lines"))

    usernames = sorted(t["username"] for t in api.token_requests)
    assert usernames == ["ws_user@interstateac|Interstate", "ws_user@interstateac|Parent"]
    rows = _rows(
        tmp_pipeline,
        "SELECT entity_id, key, txn_amount FROM journal_entry_lines ORDER BY entity_id, key",
    )
    assert [(r[0], r[1]) for r in rows] == [
        ("Interstate", "1"),
        ("Interstate", "2"),
        ("Parent", "1"),
    ]
    assert rows[2][2] == Decimal("7.25")
    table = tmp_pipeline.default_schema.get_table("journal_entry_lines")
    pk = [name for name, col in table["columns"].items() if col.get("primary_key")]
    assert sorted(pk) == ["entity_id", "key"]


@responses.activate
def test_top_level_when_no_entity_ids(tmp_pipeline: Any) -> None:
    api = FakeSageApi({("", DEPARTMENTS.object): [{"key": "12", "id": "HVAC", "name": "HVAC"}]})
    api.register(responses.mock)

    tmp_pipeline.run(_source().with_resources("departments"))

    assert [t["username"] for t in api.token_requests] == ["ws_user@interstateac"]
    assert _rows(tmp_pipeline, "SELECT entity_id, key, id FROM departments") == [("", "12", "HVAC")]
    assert "filters" not in api.queries[0][1]


@responses.activate
def test_pagination_loads_more_than_one_page(tmp_pipeline: Any) -> None:
    api = FakeSageApi({("Interstate", GL_LINES.object): [gl_line(i) for i in range(1, 1502)]})
    api.register(responses.mock)

    tmp_pipeline.run(_source(["Interstate"]).with_resources("journal_entry_lines"))

    assert [body["start"] for _, body in api.queries] == [1, 1001]
    assert _rows(tmp_pipeline, "SELECT count(*) FROM journal_entry_lines") == [(1501,)]


@responses.activate
def test_incremental_filter_uses_run_start_value(tmp_pipeline: Any) -> None:
    api = FakeSageApi(
        {
            ("Interstate", GL_LINES.object): [
                gl_line(1, "2024-01-01T00:00:00Z"),
                gl_line(2, "2024-02-01T00:00:00Z"),
            ],
            ("Parent", GL_LINES.object): [gl_line(3, "2024-01-15T00:00:00Z")],
        }
    )
    api.register(responses.mock)
    source_args = {"entity_ids": ["Interstate", "Parent"], "page_size": 1}

    tmp_pipeline.run(_source(**source_args).with_resources("journal_entry_lines"))
    first_run = [body["filters"] for _, body in api.queries]
    assert first_run
    assert all(
        f == [{"$gte": {"audit.modifiedDateTime": "2000-01-01T00:00:00Z"}}] for f in first_run
    )

    api.data[("Interstate", GL_LINES.object)].append(gl_line(4, "2024-03-01T00:00:00Z"))
    api.data[("Parent", GL_LINES.object)].append(gl_line(5, "2024-02-10T00:00:00Z"))
    api.queries.clear()
    tmp_pipeline.run(_source(**source_args).with_resources("journal_entry_lines"))

    second_run = [body["filters"][0]["$gte"]["audit.modifiedDateTime"] for _, body in api.queries]
    assert len(second_run) >= 3
    assert {str(v) for v in second_run} == {"2024-02-01T00:00:00Z"}
    keys = _rows(tmp_pipeline, "SELECT entity_id, key FROM journal_entry_lines ORDER BY 1, 2")
    assert keys == [
        ("Interstate", "1"),
        ("Interstate", "2"),
        ("Interstate", "4"),
        ("Parent", "3"),
        ("Parent", "5"),
    ]


@responses.activate
def test_zero_row_sync_materializes_declared_columns(tmp_pipeline: Any) -> None:
    api = FakeSageApi()
    api.register(responses.mock)

    tmp_pipeline.run(_source(["Interstate"]))

    assert {body["object"] for _, body in api.queries} == {obj.object for obj in OBJECTS}
    schema = tmp_pipeline.default_schema
    for obj in OBJECTS:
        columns = schema.get_table_columns(obj.resource)
        expected = {column_name(f) for f in obj.fields} | {"entity_id"}
        assert expected <= set(columns), obj.resource
