"""Integration tests for notion_source.

Mocks Notion HTTP endpoints with ``responses`` and verifies the source
materialises the expected resources with correct row counts and column
shapes when run against a DuckDB destination.

Fixture shape rationale
-----------------------
* ``users.json``    — single page, 2 users (one person, one bot)
* ``databases_page_1.json`` / ``databases_page_2.json``
    — two-page paginated search result for databases
* ``pages_page_1.json`` — single page, 2 pages
* ``blocks_page_1.json`` — top-level blocks for page-1 (page-2 gets 404)
* ``comments.json`` — one comment for block-1
* ``comments_empty.json`` — empty comments list for block-2

POST /v1/search note
--------------------
Both `databases` and `pages` resources POST to `/v1/search`.  ``responses``
dispatches POST mocks in registration order — first all databases pages, then
all pages pages — which is the order dlt calls them when it extracts the
source sequentially.

The full smoke test uses a ``responses.add_callback`` to route by request body
``filter.value`` so it is resilient to interleaving.
"""

from __future__ import annotations

import json
import logging

import responses
from requests import PreparedRequest

from paradox_dlt_sources.notion import notion_source
from tests._helpers.fixture_loader import (
    load_fixture,
    register_get,
    register_post_sequence,
)

_BASE = "https://api.notion.com"


def _make_search_callback(
    database_pages: list[dict],
    page_pages: list[dict],
) -> responses.CallbackResponse:
    """Return a ``responses`` callback that routes POST /v1/search by body filter.

    The callback tracks call counts per filter value so it can advance through
    a multi-page sequence correctly.
    """
    _counters: dict[str, int] = {"database": 0, "page": 0}
    _fixtures: dict[str, list[dict]] = {"database": database_pages, "page": page_pages}

    def _cb(request: PreparedRequest) -> tuple[int, dict, str]:
        body = json.loads(request.body or "{}")
        filter_value = (body.get("filter") or {}).get("value", "")
        idx = _counters.get(filter_value, 0)
        pages = _fixtures.get(filter_value, [{"results": [], "has_more": False}])
        fixture = pages[idx] if idx < len(pages) else {"results": [], "has_more": False}
        _counters[filter_value] = idx + 1
        return (200, {}, json.dumps(fixture))

    return _cb


def _register_full_mocks(rsps: responses.RequestsMock) -> None:
    """Register canned responses for all Notion endpoints.

    Uses a body-routing callback for POST /v1/search so ``databases`` and
    ``pages`` each get their own fixture sequence regardless of interleaving.
    """
    # users — single GET
    register_get(rsps, f"{_BASE}/v1/users", load_fixture("notion", "users"))

    # databases + pages — one callback routes by filter.value in request body
    rsps.add_callback(
        responses.POST,
        f"{_BASE}/v1/search",
        callback=_make_search_callback(
            database_pages=[
                load_fixture("notion", "databases_page_1"),
                load_fixture("notion", "databases_page_2"),
            ],
            page_pages=[load_fixture("notion", "pages_page_1")],
        ),
        content_type="application/json",
    )

    # blocks for page-1 and page-2
    # page-1 has blocks; page-2 is not shared → 404
    rsps.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-1/children",
        json=load_fixture("notion", "blocks_page_1"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-2/children",
        json={"message": "Could not find block with ID: page-2", "object": "error"},
        status=404,
    )

    # comments — block-1 has a comment, block-2 is empty
    rsps.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments"),
        status=200,
    )
    rsps.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments_empty"),
        status=200,
    )


# ---------------------------------------------------------------------------
# Full-pipeline smoke test
# ---------------------------------------------------------------------------


@responses.activate
def test_notion_source_runs_against_duckdb(tmp_pipeline):
    """Source produces all five resources without failed jobs."""
    _register_full_mocks(responses.mock)

    info = tmp_pipeline.run(notion_source(integration_token="secret_test"))

    assert not info.has_failed_jobs
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert {"users", "databases", "pages", "blocks", "comments"} <= table_names


# ---------------------------------------------------------------------------
# users resource
# ---------------------------------------------------------------------------


@responses.activate
def test_users_loads_two_rows(tmp_pipeline):
    """Users resource returns one row per workspace member."""
    register_get(responses.mock, f"{_BASE}/v1/users", load_fixture("notion", "users"))
    # Stub the other resources so they don't error
    _empty: list[dict] = [{"results": [], "has_more": False}]
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", _empty)
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", _empty)

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.users])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, type FROM users ORDER BY id")
    assert len(rows) == 2
    by_id = {r[0]: r[1] for r in rows}
    assert by_id["user-1"] == "person"
    assert by_id["user-2"] == "bot"


# ---------------------------------------------------------------------------
# databases resource
# ---------------------------------------------------------------------------


@responses.activate
def test_databases_loads_across_pages(tmp_pipeline):
    """Databases resource correctly paginates across two search result pages."""
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [
            load_fixture("notion", "databases_page_1"),
            load_fixture("notion", "databases_page_2"),
        ],
    )

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.databases])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM databases ORDER BY id")
    ids = [r[0] for r in rows]
    assert ids == ["db-1", "db-2", "db-3"]


# ---------------------------------------------------------------------------
# pages resource
# ---------------------------------------------------------------------------


@responses.activate
def test_pages_adds_page_id_alias(tmp_pipeline):
    """Each page row carries a `page_id` column that mirrors `id`."""
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [load_fixture("notion", "pages_page_1")],
    )

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, page_id FROM pages ORDER BY id")
    assert len(rows) == 2
    for row_id, page_id in rows:
        assert row_id == page_id, f"page_id alias mismatch: id={row_id} page_id={page_id}"


@responses.activate
def test_pages_stores_properties_as_json_string(tmp_pipeline):
    """Page `properties` column is stored as a JSON string, not flattened."""
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [load_fixture("notion", "pages_page_1")],
    )

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, properties FROM pages ORDER BY id")

    # properties must be a string (JSON), not a dict
    for _row_id, props in rows:
        assert isinstance(props, str), (
            f"Expected properties to be a JSON string, got {type(props).__name__}"
        )
        # Must be valid JSON with at least one key
        parsed = json.loads(props)
        assert isinstance(parsed, dict)
        assert len(parsed) > 0


# ---------------------------------------------------------------------------
# blocks resource — 404 handling
# ---------------------------------------------------------------------------


@responses.activate
def test_blocks_skips_unshared_page_and_continues(tmp_pipeline, caplog):
    """Blocks transformer skips a 404 page and continues with other pages."""
    # pages feed
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [load_fixture("notion", "pages_page_1")],
    )
    # page-1 has blocks
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-1/children",
        json=load_fixture("notion", "blocks_page_1"),
        status=200,
    )
    # page-2 is not shared → 404
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-2/children",
        json={"message": "Could not find block", "object": "error"},
        status=404,
    )
    # blocks-1 & blocks-2 each get an empty comments response
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments_empty"),
        status=200,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments_empty"),
        status=200,
    )

    source = notion_source(integration_token="secret_test")
    with caplog.at_level(logging.WARNING):
        info = tmp_pipeline.run([source.pages, source.blocks, source.comments])

    assert not info.has_failed_jobs

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM blocks ORDER BY id")
    block_ids = [r[0] for r in rows]
    # Only blocks from page-1 should appear
    assert "block-1" in block_ids
    assert "block-2" in block_ids
    # Warning logged for the skipped page
    assert any("not shared with page" in m for m in caplog.messages)


@responses.activate
def test_blocks_carry_page_id_column(tmp_pipeline):
    """Each block row has a `page_id` column matching its parent page."""
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [load_fixture("notion", "pages_page_1")],
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-1/children",
        json=load_fixture("notion", "blocks_page_1"),
        status=200,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-2/children",
        json={"object": "error"},
        status=404,
    )
    # empty comments for each block
    for _ in range(2):
        responses.mock.add(
            responses.GET,
            f"{_BASE}/v1/comments",
            json=load_fixture("notion", "comments_empty"),
            status=200,
        )

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages, source.blocks, source.comments])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, page_id FROM blocks ORDER BY id")

    assert len(rows) == 2
    for _block_id, page_id in rows:
        assert page_id == "page-1"


# ---------------------------------------------------------------------------
# comments resource
# ---------------------------------------------------------------------------


@responses.activate
def test_comments_carry_block_id_column(tmp_pipeline):
    """Each comment row has a `block_id` column matching its parent block."""
    register_post_sequence(
        responses.mock,
        f"{_BASE}/v1/search",
        [load_fixture("notion", "pages_page_1")],
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-1/children",
        json=load_fixture("notion", "blocks_page_1"),
        status=200,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-2/children",
        json={"object": "error"},
        status=404,
    )
    # block-1 gets a comment; block-2 gets empty
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments"),
        status=200,
    )
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments_empty"),
        status=200,
    )

    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages, source.blocks, source.comments])

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id, block_id FROM comments ORDER BY id")

    assert len(rows) == 1
    comment_id, block_id = rows[0]
    assert comment_id == "comment-1"
    assert block_id == "block-1"
