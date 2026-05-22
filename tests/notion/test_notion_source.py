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

import pytest
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


# ---------------------------------------------------------------------------
# databases — cursor early-termination (lines 265-266, 269)
# ---------------------------------------------------------------------------


@responses.activate
def test_databases_early_termination_stops_at_threshold(tmp_pipeline):
    """databases stops yielding once last_edited_time falls at/below cursor threshold.

    Run 1: seeds cursor with last_edited_time = "2026-05-15T12:00:00.000Z" (db-1).
    Run 2: server returns db-new (above threshold) and db-1 (== threshold).
           db-1 must not be yielded on run 2 — early-term triggers on it.
    """
    # ── Run 1: single database, seeds cursor ──────────────────────────────
    _page1 = {
        "object": "list",
        "results": [
            {
                "object": "database",
                "id": "db-1",
                "created_time": "2026-03-01T09:00:00.000Z",
                "last_edited_time": "2026-05-15T12:00:00.000Z",
                "archived": False,
                "url": "https://www.notion.so/db-1",
                "parent": {"type": "workspace", "workspace": True},
            }
        ],
        "next_cursor": None,
        "has_more": False,
    }
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_page1])
    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.databases])

    # ── Run 2: threshold == "2026-05-15T12:00:00.000Z" ───────────────────
    # db-new is above threshold; db-1 is at threshold → triggers stop
    _page2 = {
        "object": "list",
        "results": [
            {
                "object": "database",
                "id": "db-new",
                "created_time": "2026-05-20T00:00:00.000Z",
                "last_edited_time": "2026-05-20T10:00:00.000Z",  # above
                "archived": False,
                "url": "https://www.notion.so/db-new",
                "parent": {"type": "workspace", "workspace": True},
            },
            {
                "object": "database",
                "id": "db-1",
                "created_time": "2026-03-01T09:00:00.000Z",
                "last_edited_time": "2026-05-15T12:00:00.000Z",  # == threshold
                "archived": False,
                "url": "https://www.notion.so/db-1",
                "parent": {"type": "workspace", "workspace": True},
            },
        ],
        "next_cursor": None,
        "has_more": False,
    }
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_page2])
    source2 = notion_source(integration_token="secret_test")
    info = tmp_pipeline.run([source2.databases])
    assert not info.has_failed_jobs

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM databases ORDER BY id")
    ids = [r[0] for r in rows]
    assert "db-new" in ids
    # db-1 was only yielded on run 1; run 2 must not yield it again (== threshold)
    assert ids.count("db-1") == 1


# ---------------------------------------------------------------------------
# pages — cursor early-termination (lines 309-310, 315)
# ---------------------------------------------------------------------------


@responses.activate
def test_pages_early_termination_stops_at_threshold(tmp_pipeline):
    """pages stops yielding once last_edited_time falls at/below cursor threshold.

    Same two-run strategy as the databases test above.
    """
    # ── Run 1: seed cursor ────────────────────────────────────────────────
    _seed_page = {
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": "page-alpha",
                "created_time": "2026-04-01T10:00:00.000Z",
                "last_edited_time": "2026-05-10T14:00:00.000Z",
                "archived": False,
                "url": "https://www.notion.so/page-alpha",
                "parent": {"type": "workspace", "workspace": True},
                "created_by": {"object": "user", "id": "user-1"},
                "last_edited_by": {"object": "user", "id": "user-1"},
                "properties": {},
            }
        ],
        "next_cursor": None,
        "has_more": False,
    }
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_seed_page])
    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages])

    # ── Run 2: threshold == "2026-05-10T14:00:00.000Z" ───────────────────
    _run2_page = {
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": "page-beta",
                "created_time": "2026-05-15T00:00:00.000Z",
                "last_edited_time": "2026-05-19T08:00:00.000Z",  # above threshold
                "archived": False,
                "url": "https://www.notion.so/page-beta",
                "parent": {"type": "workspace", "workspace": True},
                "created_by": {"object": "user", "id": "user-1"},
                "last_edited_by": {"object": "user", "id": "user-1"},
                "properties": {},
            },
            {
                "object": "page",
                "id": "page-alpha",
                "created_time": "2026-04-01T10:00:00.000Z",
                "last_edited_time": "2026-05-10T14:00:00.000Z",  # == threshold → stop
                "archived": False,
                "url": "https://www.notion.so/page-alpha",
                "parent": {"type": "workspace", "workspace": True},
                "created_by": {"object": "user", "id": "user-1"},
                "last_edited_by": {"object": "user", "id": "user-1"},
                "properties": {},
            },
        ],
        "next_cursor": None,
        "has_more": False,
    }
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_run2_page])
    source2 = notion_source(integration_token="secret_test")
    info = tmp_pipeline.run([source2.pages])
    assert not info.has_failed_jobs

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM pages ORDER BY id")
    ids = [r[0] for r in rows]
    assert "page-beta" in ids
    # page-alpha only from run 1; == threshold on run 2 → not yielded again
    assert ids.count("page-alpha") == 1


# ---------------------------------------------------------------------------
# blocks — stale-block skip (line 356) + non-404 reraise (line 367)
# ---------------------------------------------------------------------------


@responses.activate
def test_blocks_skips_stale_blocks_below_threshold(tmp_pipeline):
    """blocks skips individual blocks whose last_edited_time is at/below threshold.

    Run 1 seeds the cursor. Run 2 has one fresh block and one stale block;
    only the fresh block should be yielded.
    """
    _page_fixture = {
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": "page-x",
                "created_time": "2026-04-01T10:00:00.000Z",
                "last_edited_time": "2026-05-10T14:00:00.000Z",
                "archived": False,
                "url": "https://www.notion.so/page-x",
                "parent": {"type": "workspace", "workspace": True},
                "created_by": {"object": "user", "id": "user-1"},
                "last_edited_by": {"object": "user", "id": "user-1"},
                "properties": {},
            }
        ],
        "next_cursor": None,
        "has_more": False,
    }
    _blocks_seed = {
        "object": "list",
        "results": [
            {
                "object": "block",
                "id": "block-seed",
                "type": "paragraph",
                "created_time": "2026-04-01T10:00:00.000Z",
                "last_edited_time": "2026-05-08T09:00:00.000Z",
                "archived": False,
                "has_children": False,
                "parent": {"type": "page_id", "page_id": "page-x"},
            }
        ],
        "next_cursor": None,
        "has_more": False,
    }
    _comments_empty = load_fixture("notion", "comments_empty")

    # ── Run 1: seed blocks cursor ─────────────────────────────────────────
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_page_fixture])
    responses.mock.add(responses.GET, f"{_BASE}/v1/blocks/page-x/children", json=_blocks_seed)
    responses.mock.add(responses.GET, f"{_BASE}/v1/comments", json=_comments_empty)
    source = notion_source(integration_token="secret_test")
    tmp_pipeline.run([source.pages, source.blocks, source.comments])

    # ── Run 2: threshold = "2026-05-08T09:00:00.000Z" ────────────────────
    # The page fixture's last_edited_time must advance past the prior run's
    # cursor or the pages resource (range_start="open") will skip page-x and
    # the blocks transformer won't fire at all — masking what we're trying to
    # test (the per-block stale skip at line 356).
    _page_fixture_run2 = {
        **_page_fixture,
        "results": [
            {**_page_fixture["results"][0], "last_edited_time": "2026-05-15T14:00:00.000Z"}
        ],
    }
    _blocks_run2 = {
        "object": "list",
        "results": [
            {
                "object": "block",
                "id": "block-fresh",
                "type": "paragraph",
                "created_time": "2026-05-20T00:00:00.000Z",
                "last_edited_time": "2026-05-20T10:00:00.000Z",  # above threshold
                "archived": False,
                "has_children": False,
                "parent": {"type": "page_id", "page_id": "page-x"},
            },
            {
                "object": "block",
                "id": "block-seed",
                "type": "paragraph",
                "created_time": "2026-04-01T10:00:00.000Z",
                "last_edited_time": "2026-05-08T09:00:00.000Z",  # == threshold → skip
                "archived": False,
                "has_children": False,
                "parent": {"type": "page_id", "page_id": "page-x"},
            },
        ],
        "next_cursor": None,
        "has_more": False,
    }
    register_post_sequence(responses.mock, f"{_BASE}/v1/search", [_page_fixture_run2])
    responses.mock.add(responses.GET, f"{_BASE}/v1/blocks/page-x/children", json=_blocks_run2)
    # fresh block gets a comments call; stale block is skipped before comments
    responses.mock.add(responses.GET, f"{_BASE}/v1/comments", json=_comments_empty)
    source2 = notion_source(integration_token="secret_test")
    info = tmp_pipeline.run([source2.pages, source2.blocks, source2.comments])
    assert not info.has_failed_jobs

    with tmp_pipeline.sql_client() as client:
        rows = client.execute_sql("SELECT id FROM blocks ORDER BY id")
    ids = [r[0] for r in rows]
    assert "block-fresh" in ids
    # block-seed was only yielded on run 1; run 2 skips it (== threshold)
    assert ids.count("block-seed") == 1


@responses.activate
def test_blocks_reraises_on_non_404():
    """A non-404 HTTP error from the blocks endpoint propagates (line 367).

    Tested via direct iteration so the exception isn't swallowed by dlt's
    pipeline-level error wrapping. The blocks transformer takes a page row
    as input; we feed it a synthetic page directly.
    """
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/blocks/page-y/children",
        json={"object": "error", "status": 500, "code": "internal_server_error", "message": "An unexpected error occurred"},
        status=500,
    )

    source = notion_source(integration_token="secret_test")
    page_row = {"id": "page-y", "last_edited_time": "2026-05-10T14:00:00.000Z"}
    with pytest.raises(Exception, match="blocks|500"):
        list(source.blocks(page_row))


# ---------------------------------------------------------------------------
# comments — 404 skip (lines 400-403) + non-404 reraise
# ---------------------------------------------------------------------------


@responses.activate
def test_comments_skips_on_404(tmp_pipeline):
    """comments silently skips a block when the comments endpoint returns 404.

    Notion returns 404 when the integration does not have comment access for
    the parent page — not a fatal error.
    """
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
        json={"object": "error", "status": 404, "code": "object_not_found", "message": "Could not find block with ID: page-2"},
        status=404,
    )
    # block-1 → 404 on comments (no comment access)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json={"object": "error", "status": 404, "code": "object_not_found", "message": "Could not find block with ID: block-1"},
        status=404,
    )
    # block-2 → empty comments (normal)
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json=load_fixture("notion", "comments_empty"),
        status=200,
    )

    source = notion_source(integration_token="secret_test")
    info = tmp_pipeline.run([source.pages, source.blocks, source.comments])
    assert not info.has_failed_jobs
    # No comment rows — the 404 was skipped
    table_names = {t["name"] for t in tmp_pipeline.default_schema.data_tables()}
    assert "comments" not in table_names


@responses.activate
def test_comments_reraises_on_non_404():
    """comments re-raises on non-404 errors (line 403).

    Direct-iter to bypass dlt's pipeline-level error wrapping. The comments
    transformer takes a block row as input.
    """
    responses.mock.add(
        responses.GET,
        f"{_BASE}/v1/comments",
        json={
            "object": "error",
            "status": 400,
            "code": "unauthorized_capability",
            "message": "The provided token does not have the required capability to perform this action.",
        },
        status=400,
    )

    source = notion_source(integration_token="secret_test")
    block_row = {"id": "block-1", "page_id": "page-1"}
    with pytest.raises(Exception, match="comments|400"):
        list(source.comments(block_row))
