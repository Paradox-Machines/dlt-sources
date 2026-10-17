"""Unit tests for raisers_edge helpers: auth, transforms, cursor window, quota."""

from __future__ import annotations

from datetime import date, timedelta
from urllib.parse import parse_qs

import pendulum
import pytest
import requests
import responses

from paradox_dlt_sources.raisers_edge.helpers import (
    ChangeWindow,
    SkyOAuth2Auth,
    SkyQuotaExceededError,
    normalize_row,
    raise_on_quota,
    reconciliation_due,
    to_utc,
)

TOKEN_URL = "https://oauth2.sky.blackbaud.com/token"


def _auth(preserve: bool, rotated: list[str]) -> SkyOAuth2Auth:
    return SkyOAuth2Auth(
        token_url=TOKEN_URL,
        client_id="cid",
        client_secret="csec",
        refresh_token="old",
        subscription_key="subkey",
        preserve_refresh_token=preserve,
        on_token_rotation=rotated.append,
    )


@responses.activate
def test_auth_sets_headers_and_preserves_refresh_token() -> None:
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={
            "access_token": "tok",
            "expires_in": 3599,
            "refresh_token": "old",
            "environment_id": "p-env",
        },
    )
    responses.add(responses.GET, "https://api.sky.blackbaud.com/x", json={})
    rotated: list[str] = []
    auth = _auth(True, rotated)
    requests.get("https://api.sky.blackbaud.com/x", auth=auth, timeout=5)
    requests.get("https://api.sky.blackbaud.com/x", auth=auth, timeout=5)

    token_calls = [c for c in responses.calls if c.request.url == TOKEN_URL]
    assert len(token_calls) == 1
    form = parse_qs(token_calls[0].request.body)
    assert form["preserve_refresh_token"] == ["true"]
    assert form["grant_type"] == ["refresh_token"]
    api = responses.calls[-1].request
    assert api.headers["Authorization"] == "Bearer tok"
    assert api.headers["Bb-Api-Subscription-Key"] == "subkey"
    assert auth.environment_id == "p-env"
    assert rotated == []


@responses.activate
def test_auth_reports_rotation_when_not_preserving() -> None:
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok", "expires_in": 3599, "refresh_token": "new"},
    )
    responses.add(responses.GET, "https://api.sky.blackbaud.com/x", json={})
    rotated: list[str] = []
    requests.get("https://api.sky.blackbaud.com/x", auth=_auth(False, rotated), timeout=5)
    assert "preserve_refresh_token" not in parse_qs(responses.calls[0].request.body)
    assert rotated == ["new"]


def test_normalize_row_dates_and_standard_columns() -> None:
    now = pendulum.datetime(2026, 10, 16, tz="UTC")
    row = normalize_row(
        {
            "id": "1",
            "date": "2024-05-01T00:00:00",
            "completed_date": "2024-05-01T09:30:00",
            "date_added": "2020-01-01T10:00:00-05:00",
            "date_modified": "2026-10-01T12:00:00-04:00",
            "amount": {"value": 10.5},
        },
        tz="America/New_York",
        date_fields=("date",),
        extracted_at=now,
        currency_code="USD",
    )
    assert row["date"] == date(2024, 5, 1)
    assert row["completed_date"] == pendulum.datetime(2024, 5, 1, 13, 30, tz="UTC")
    assert row["_source_created_at"] == pendulum.datetime(2020, 1, 1, 15, tz="UTC")
    assert row["_source_updated_at"] == pendulum.datetime(2026, 10, 1, 16, tz="UTC")
    assert row["_is_deleted"] is False
    assert row["_deleted_at"] is None
    assert row["_extracted_at"] == now
    assert row["currency_code"] == "USD"
    assert row["amount__value"] == 10.5
    assert "amount" not in row
    assert row["date_modified"] == "2026-10-01T12:00:00-04:00"


def test_to_utc_none_and_empty() -> None:
    assert to_utc(None, "UTC") is None
    assert to_utc("", "UTC") is None


def test_change_window_floor_and_lookback() -> None:
    state: dict[str, str] = {"cursor": "2026-10-01T12:00:00+00:00"}
    window = ChangeWindow(state, timedelta(hours=1))
    assert window.floor(None) == pendulum.datetime(2026, 10, 1, 11, tz="UTC")
    assert ChangeWindow({}, timedelta(hours=1)).floor(None) is None
    start = pendulum.datetime(2025, 1, 1, tz="UTC")
    assert ChangeWindow({}, timedelta(hours=1)).floor(start) == start


def test_change_window_drops_duplicates_and_holds_cursor_back() -> None:
    state: dict[str, str] = {}
    window = ChangeWindow(state, timedelta(hours=1))
    t1 = pendulum.datetime(2026, 10, 1, 10, tz="UTC")
    t2 = pendulum.datetime(2026, 10, 1, 11, tz="UTC")
    t3 = pendulum.datetime(2026, 10, 1, 12, tz="UTC")
    assert window.admit(("a",), t1)
    assert window.admit(("b",), t3)
    assert not window.admit(("a",), t2)
    window.commit()
    assert state["cursor"] == t2.isoformat()


def test_change_window_commit_without_rows_keeps_cursor() -> None:
    state = {"cursor": "2026-10-01T12:00:00+00:00"}
    window = ChangeWindow(state, timedelta(hours=1))
    window.commit()
    assert state["cursor"] == "2026-10-01T12:00:00+00:00"


def test_reconciliation_due() -> None:
    now = pendulum.datetime(2026, 10, 16, tz="UTC")
    assert not reconciliation_due({}, timedelta(days=1), now)
    assert not reconciliation_due({"last_reconciled_at": now.isoformat()}, None, now)
    past = now.subtract(days=2).isoformat()
    assert reconciliation_due({"last_reconciled_at": past}, timedelta(days=1), now)
    recent = now.subtract(hours=2).isoformat()
    assert not reconciliation_due({"last_reconciled_at": recent}, timedelta(days=1), now)


def test_raise_on_quota() -> None:
    response = requests.Response()
    response.status_code = 403
    response._content = (
        b'{"message":"Out of call volume quota. Quota will be replenished in 01:00:00"}'
    )
    response.headers["Retry-After"] = "3600"
    with pytest.raises(SkyQuotaExceededError, match="3600"):
        raise_on_quota(response)
    forbidden = requests.Response()
    forbidden.status_code = 403
    forbidden._content = b'{"message":"Forbidden"}'
    raise_on_quota(forbidden)
