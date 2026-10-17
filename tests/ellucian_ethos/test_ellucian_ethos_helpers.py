"""Unit tests for ellucian_ethos helpers: token refresh, paging and transforms."""

from __future__ import annotations

import datetime as dt
from typing import Any

import requests
import responses

from paradox_dlt_sources.ellucian_ethos import helpers
from paradox_dlt_sources.ellucian_ethos.helpers import (
    EthosOffsetPaginator,
    EthosTokenAuth,
    change_position,
    parse_timestamp,
    transform_row,
)

_BASE = "https://integrate.elluciancloud.test"


def _response(headers: dict[str, str]) -> requests.Response:
    response = requests.Response()
    response.headers.update(headers)
    response.status_code = 200
    return response


def test_token_is_cached_then_refreshed_before_expiry(monkeypatch: Any) -> None:
    clock = [1000.0]
    monkeypatch.setattr(helpers.time, "time", lambda: clock[0])
    auth = EthosTokenAuth(_BASE, "key")
    with responses.RequestsMock() as rsps:
        rsps.add(responses.POST, f"{_BASE}/auth", body="t1")
        rsps.add(responses.POST, f"{_BASE}/auth", body='"t2"')
        assert auth.token() == "t1"
        clock[0] += 200
        assert auth.token() == "t1"
        clock[0] += 50  # 250s after issue: inside the 60s safety margin of a 300s token
        assert auth.token() == "t2"
        assert len(rsps.calls) == 2
    assert "key" not in repr(auth)


def test_paginator_without_total_stops_on_empty_page() -> None:
    paginator = EthosOffsetPaginator(2)
    paginator.update_state(_response({}), [{"id": "a"}, {"id": "b"}])
    assert paginator.has_next_page and paginator.offset == 2
    paginator.update_state(_response({}), [])
    assert not paginator.has_next_page


def test_paginator_flags_restricted_content_and_reads_media_type() -> None:
    paginator = EthosOffsetPaginator(10)
    paginator.update_state(
        _response(
            {
                "X-Total-Count": "1",
                "X-Media-Type": "application/vnd.hedtech.integration.v12+json",
                "X-Content-Restricted": "partial",
            }
        ),
        [{"id": "a"}],
    )
    assert not paginator.has_next_page
    assert paginator.content_restricted
    assert paginator.media_type == "application/vnd.hedtech.integration.v12+json"


def test_transform_row_reads_metadata_timestamps() -> None:
    now = dt.datetime(2026, 10, 16, tzinfo=dt.UTC)
    row = transform_row(
        {"id": "a", "metadata": {"createdOn": "2026-01-01T00:00:00-05:00"}}, "v", now
    )
    assert row["_source_created_at"] == dt.datetime(2026, 1, 1, 5, tzinfo=dt.UTC)
    assert row["_source_updated_at"] is None
    assert row["_is_deleted"] is False and row["_extracted_at"] == now


def test_change_position_sorts_as_text() -> None:
    assert change_position(9) < change_position(10)
    assert parse_timestamp(None) is None
